import csv
import io
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

    def invoice_key_from_issues(row: Dict[str, Any]) -> str:
    """Key function specialised for the Issues master."""
    return (
        norm(
            row.get("Invoice_Key")
            or row.get("Invoice key")
            or row.get("Key")
            or row.get("Invoice ID")
            or row.get("Invoice Number")
            or row.get("Invoice number")
            or row.get("Xero number")
        )
        .upper()
    )

    Actor.log.info(f"Downloading {label} CSV from {url}")

    with urllib.request.urlopen(url) as resp:
        csv_bytes = resp.read()

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


def invoice_key_generic(row: Dict[str, Any]) -> str:
    """Build a robust invoice key from whatever invoice/Xero fields exist."""
    return (
        norm(
            row.get("Invoice Number")
            or row.get("Invoice number")
            or row.get("Invoice No")
            or row.get("Invoice")
            or row.get("Xero number")
            or row.get("Xero Number")
            or row.get("Invoice ID")
            or row.get("InvoiceID")
            or row.get("Xero Invoice ID")
        )
        .upper()
    )


from typing import Callable  # add at the top with the other imports


def build_map_with_key(
    rows: List[Dict[str, Any]],
    key_fn: Callable[[Dict[str, Any]], str],
) -> Tuple[Dict[str, List[Dict[str, Any]]], Set[str]]:
    m: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        k = key_fn(r)
        if not k:
            continue
        m.setdefault(k, []).append(r)
    return m, set(m.keys())


def pick_first(row_list: List[Dict[str, Any]], *field_candidates: str) -> str:
    """Pick first non-empty field from the first row in a list, checking several names."""
    if not row_list:
        return ""
    row = row_list[0]
    for f in field_candidates:
        v = row.get(f)
        if v not in (None, ""):
            return norm(v)
    return ""


# ----------------- Apify entrypoint -----------------


async def main() -> None:
    async with Actor:
        actor_input = await Actor.get_input() or {}
        Actor.log.info(f"Actor input keys: {list(actor_input.keys())}")

        year = norm(actor_input.get("Year"))
        invoices_url = norm(actor_input.get("InvoicesUrl"))
        attach_url = norm(actor_input.get("AttachUrl"))
        issues_url = norm(actor_input.get("IssuesUrl"))

        if not year:
            Actor.log.error("Missing 'Year' in input.")
            return

        if not invoices_url:
            Actor.log.error("InvoicesUrl is required; cannot build enriched master.")
            return

        # 1) Download CSVs
        invoices_rows = download_csv(invoices_url, "invoices")
        attach_rows = download_csv(attach_url, "attachments") if attach_url else []
        issues_rows = download_csv(issues_url, "issues") if issues_url else []

        if not invoices_rows:
            Actor.log.error("Invoice master CSV is empty or failed to parse; aborting.")
            return

        Actor.log.info(
            f"Row counts: invoices={len(invoices_rows)}, "
            f"attachments={len(attach_rows)}, issues={len(issues_rows)}"
        )

        # 2) Build maps by invoice key (different key fns per source)
        attach_map, attach_keys = build_map_with_key(attach_rows, invoice_key_generic)
        issues_map, issues_keys = build_map_with_key(issues_rows, invoice_key_from_issues)

        Actor.log.info(
            f"Key coverage: attach_keys={len(attach_keys)}, issues_keys={len(issues_keys)}"
        )

        # 3) Prepare output field order:
        #    preserve invoice CSV column order, then append enrichment fields.
        invoice_fieldnames = list(invoices_rows[0].keys())
        enrich_fields = [
            "Has_Attachments",
            "Attachment_Count",
            "Sample_Attach_Name",
            "Has_Issues",
            "Issue_Count",
            "Sample_Issue_Desc",
        ]
        fieldnames = invoice_fieldnames + enrich_fields

        # 4) Build enriched rows
        enriched_rows: List[Dict[str, Any]] = []

        for inv in invoices_rows:
            key = invoice_key_generic(inv)
            a_rows = attach_map.get(key, [])
            i_rows = issues_map.get(key, [])

            attach_count = len(a_rows)
            issue_count = len(i_rows)

            sample_attach_name = pick_first(
                a_rows,
                "File name",
                "Filename",
                "Name",
                "Attachment name",
                "Attachment",
            )
            sample_issue_desc = pick_first(
                i_rows,
                "Issue_Flag",
                "Issue",
                "Untracked reason",
                "Reason",
                "Issue description",
            )

            out = dict(inv)  # copy original invoice row
            out["Has_Attachments"] = "Y" if attach_count > 0 else "N"
            out["Attachment_Count"] = attach_count
            out["Sample_Attach_Name"] = sample_attach_name
            out["Has_Issues"] = "Y" if issue_count > 0 else "N"
            out["Issue_Count"] = issue_count
            out["Sample_Issue_Desc"] = sample_issue_desc

            enriched_rows.append(out)

        Actor.log.info(f"Enriched rows: {len(enriched_rows)}")

        # 5) Write CSV to KV store
        filename = f"invoice_master_enriched_{year}.csv"
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in enriched_rows:
            writer.writerow(row)

        csv_data = buf.getvalue()

        await Actor.set_value(
            filename,
            csv_data,
            content_type="text/csv; charset=utf-8",
        )

        # 6) Push a small JSON summary to dataset
        summary = {
            "year": year,
            "invoice_rows": len(invoices_rows),
            "attachment_rows": len(attach_rows),
            "issue_rows": len(issues_rows),
            "enriched_rows": len(enriched_rows),
            "kv_filename": filename,
        }
        await Actor.push_data(summary)

        Actor.log.info(
            f"Done. Year={year}, enriched_rows={len(enriched_rows)}, "
            f"kv_file={filename}"
        )


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
