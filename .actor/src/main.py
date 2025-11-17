import csv
import io
import json
import urllib.request
from typing import List, Dict, Any, Tuple, Set

from apify import Actor


# ----------------- small helpers -----------------


def norm(s: Any) -> str:
    if s is None:
        return ""
    return str(s).strip()


def download_csv(url: str, label: str) -> List[Dict[str, Any]]:
    """Download a CSV from Dropbox and parse it into a list of dicts.

    Primary attempt: csv.DictReader (normal).
    Fallback: very simple split-based parser if DictReader chokes (e.g. bad newlines).
    """
    if not url:
        return []

    Actor.log.info(f"Downloading {label} CSV from {url}")

    with urllib.request.urlopen(url) as resp:
        csv_bytes = resp.read()

    # Decode text
    text = csv_bytes.decode("utf-8", errors="replace")

    rows: List[Dict[str, Any]] = []

    # First try: normal DictReader
    try:
        reader = csv.DictReader(io.StringIO(text))
        rows = [dict(r) for r in reader]
        Actor.log.info(f"{label} rows (DictReader): {len(rows)}")
        return rows
    except csv.Error as e:
        Actor.log.warning(
            f"{label} CSV parse via DictReader failed: {e!r}. "
            f"Falling back to simple split parser; some rows may be skipped."
        )

    # Fallback: naive split-by-line, split-by-comma parser.
    lines = [ln for ln in text.splitlines() if ln.strip()]
    if not lines:
        Actor.log.warning(f"{label} CSV has no non-empty lines after fallback parsing.")
        return []

    header = [h.strip() for h in lines[0].split(",")]
    for ln in lines[1:]:
        parts = [p.strip() for p in ln.split(",")]
        if len(parts) != len(header):
            # Skip malformed lines in fallback mode
            continue
        row = dict(zip(header, parts))
        rows.append(row)

    Actor.log.info(f"{label} rows (fallback): {len(rows)}")
    return rows


def invoice_key_from_ledger(row: Dict[str, Any]) -> str:
    # Typical ledger header: "Invoice Number"
    return norm(
        row.get("Invoice Number")
        or row.get("Invoice number")
        or row.get("Invoice No")
        or row.get("Invoice")
    ).upper()


def invoice_key_from_master(row: Dict[str, Any]) -> str:
    # Your master financials: "Xero number" or "Invoice Number"
    return norm(
        row.get("Xero number")
        or row.get("Invoice Number")
        or row.get("Invoice number")
        or row.get("Invoice")
    ).upper()


def invoice_key_from_invoices(row: Dict[str, Any]) -> str:
    # Invoice master CSV: usually "Invoice Number"
    return norm(
        row.get("Invoice Number")
        or row.get("Invoice number")
        or row.get("Xero number")
        or row.get("Invoice")
    ).upper()


def build_coverage(
    ledger_rows: List[Dict[str, Any]],
    master_rows: List[Dict[str, Any]],
    invoice_rows: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], Set[str], Set[str], Set[str]]:
    """Build invoice-level coverage rows and return (rows, ledger_keys, master_keys, invoice_keys)."""

    ledger_map: Dict[str, List[Dict[str, Any]]] = {}
    master_map: Dict[str, List[Dict[str, Any]]] = {}
    invoice_map: Dict[str, List[Dict[str, Any]]] = {}

    # Index ledger
    for r in ledger_rows:
        k = invoice_key_from_ledger(r)
        if not k:
            continue
        ledger_map.setdefault(k, []).append(r)

    # Index master financials
    for r in master_rows:
        k = invoice_key_from_master(r)
        if not k:
            continue
        master_map.setdefault(k, []).append(r)

    # Index invoice master
    for r in invoice_rows:
        k = invoice_key_from_invoices(r)
        if not k:
            continue
        invoice_map.setdefault(k, []).append(r)

    ledger_keys = set(ledger_map.keys())
    master_keys = set(master_map.keys())
    invoice_keys = set(invoice_map.keys())

    all_keys = ledger_keys | master_keys | invoice_keys

    coverage_rows: List[Dict[str, Any]] = []

    for inv_key in sorted(all_keys):
        l_rows = ledger_map.get(inv_key, [])
        m_rows = master_map.get(inv_key, [])
        i_rows = invoice_map.get(inv_key, [])

        in_ledger = bool(l_rows)
        in_master = bool(m_rows)
        in_invoices = bool(i_rows)

        # Very simple issue classification
        if in_ledger and in_master and in_invoices:
            issue = "OK_ALL_THREE"
        elif in_ledger and in_master and not in_invoices:
            issue = "NO_INVOICE_MASTER"
        elif in_ledger and not in_master and in_invoices:
            issue = "NO_MASTER_FINANCIALS"
        elif not in_ledger and in_master and in_invoices:
            issue = "MISSING_LEDGER"
        elif in_ledger and not in_master and not in_invoices:
            issue = "LEDGER_ONLY"
        elif not in_ledger and in_master and not in_invoices:
            issue = "MASTER_ONLY"
        elif not in_ledger and not in_master and in_invoices:
            issue = "INVOICE_ONLY"
        else:
            issue = "UNKNOWN_PATTERN"

        # Sample description fields
        sample_ledger_desc = norm(
            (l_rows[0].get("Description") if l_rows else "")
            or (l_rows[0].get("Narration") if l_rows else "")
        )
        sample_master_desc = norm(
            (m_rows[0].get("Description") if m_rows else "")
            or (m_rows[0].get("Contact") if m_rows else "")
        )
        sample_invoice_desc = norm(
            (i_rows[0].get("Description") if i_rows else "")
            or (i_rows[0].get("Contact") if i_rows else "")
        )

        coverage_rows.append(
            {
                "Invoice_Key": inv_key,
                "In_Ledger": "Y" if in_ledger else "N",
                "In_MasterFinancials": "Y" if in_master else "N",
                "In_InvoiceMaster": "Y" if in_invoices else "N",
                "Ledger_Row_Count": len(l_rows),
                "Master_Line_Count": len(m_rows),
                "Invoice_Line_Count": len(i_rows),
                "Issue_Flag": issue,
                "Sample_Ledger_Desc": sample_ledger_desc,
                "Sample_Master_Desc": sample_master_desc,
                "Sample_Invoice_Desc": sample_invoice_desc,
            }
        )

    return coverage_rows, ledger_keys, master_keys, invoice_keys


# ----------------- Apify entrypoint -----------------


async def main() -> None:
    async with Actor:
        actor_input = await Actor.get_input() or {}
        Actor.log.info(f"Actor input keys: {list(actor_input.keys())}")

        year = norm(actor_input.get("Year"))
        ledger_url = norm(actor_input.get("LedgerUrl"))
        master_url = norm(actor_input.get("MasterUrl"))
        invoices_url = norm(actor_input.get("InvoicesUrl"))

        if not year:
            Actor.log.error("Missing 'Year' in input.")
            return

        if not (ledger_url or master_url or invoices_url):
            Actor.log.error("No URLs provided (LedgerUrl/MasterUrl/InvoicesUrl). Nothing to do.")
            return

        # 1) Download CSVs
        ledger_rows: List[Dict[str, Any]] = []
        master_rows: List[Dict[str, Any]] = []
        invoice_rows: List[Dict[str, Any]] = []

        if ledger_url:
            ledger_rows = download_csv(ledger_url, "ledger")
        else:
            Actor.log.warning("No LedgerUrl provided; skipping ledger.")

        if master_url:
            master_rows = download_csv(master_url, "master")
        else:
            Actor.log.warning("No MasterUrl provided; skipping master financials.")

        if invoices_url:
            invoice_rows = download_csv(invoices_url, "invoices")
        else:
            Actor.log.warning("No InvoicesUrl provided; skipping invoice master.")

        total_rows = len(ledger_rows) + len(master_rows) + len(invoice_rows)
        if total_rows == 0:
            Actor.log.error("All CSVs are empty or failed to parse; aborting.")
            return

        Actor.log.info(
            f"Row counts: ledger={len(ledger_rows)}, "
            f"master={len(master_rows)}, invoices={len(invoice_rows)}"
        )

        # 2) Build coverage
        coverage_rows, ledger_keys, master_keys, invoice_keys = build_coverage(
            ledger_rows, master_rows, invoice_rows
        )

        Actor.log.info(
            f"Key coverage: ledger_keys={len(ledger_keys)}, "
            f"master_keys={len(master_keys)}, invoice_keys={len(invoice_keys)}, "
            f"coverage_rows={len(coverage_rows)}"
        )

        # 3) Write CSV to KV store
        filename = f"tx_master_audit_{year}.csv"

        fieldnames = [
            "Year",
            "Invoice_Key",
            "In_Ledger",
            "In_MasterFinancials",
            "In_InvoiceMaster",
            "Ledger_Row_Count",
            "Master_Line_Count",
            "Invoice_Line_Count",
            "Issue_Flag",
            "Sample_Ledger_Desc",
            "Sample_Master_Desc",
            "Sample_Invoice_Desc",
        ]

        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in coverage_rows:
            out = dict(row)
            out["Year"] = year
            writer.writerow(out)

        csv_data = buf.getvalue()

        await Actor.set_value(
            filename,
            csv_data,
            content_type="text/csv; charset=utf-8",
        )

        # 4) Push a small JSON summary to dataset so Apify "Get dataset" is non-empty
        summary = {
            "year": year,
            "ledger_rows": len(ledger_rows),
            "master_rows": len(master_rows),
            "invoice_rows": len(invoice_rows),
            "invoice_keys_total": len(ledger_keys | master_keys | invoice_keys),
            "coverage_rows": len(coverage_rows),
            "kv_filename": filename,
        }
        await Actor.push_data(summary)

        Actor.log.info(
            f"Done. Year={year}, coverage_rows={len(coverage_rows)}, "
            f"kv_file={filename}"
        )


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
