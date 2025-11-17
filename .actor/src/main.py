import csv
import io
import urllib.request
from typing import List, Dict, Any, Tuple, Set

from apify import Actor


# ---------- small helpers ----------

def norm(s: Any) -> str:
    """Trim and stringify, never return None."""
    if s is None:
        return ""
    return str(s).strip()


def download_csv(url: str, label: str) -> List[Dict[str, Any]]:
    """Download a CSV from Dropbox and parse it into a list of dicts.

    First try csv.DictReader. If that fails (bad newlines, etc),
    fall back to a very simple split-based parser.
    """
    if not url:
        return []

    Actor.log.info(f"Downloading {label} CSV from {url}")

    with urllib.request.urlopen(url) as resp:
        csv_bytes = resp.read()

    text = csv_bytes.decode("utf-8", errors="replace")

    # Primary: DictReader
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

    # Fallback parser
    lines = [ln for ln in text.splitlines() if ln.strip()]
    if not lines:
        Actor.log.warning(f"{label} CSV has no non-empty lines after fallback parsing.")
        return []

    header = [h.strip() for h in lines[0].split(",")]
    rows: List[Dict[str, Any]] = []
    for ln in lines[1:]:
        parts = [p.strip() for p in ln.split(",")]
        if len(parts) != len(header):
            continue
        rows.append(dict(zip(header, parts)))

    Actor.log.info(f"{label} rows (fallback): {len(rows)}")
    return rows


# ---------- key functions ----------

def key_from_invoice(row: Dict[str, Any]) -> str:
    """Key for the Invoice master – prefer Invoice ID."""
    return norm(
        row.get("Invoice ID")
        or row.get("InvoiceID")
        or row.get("Xero number")
        or row.get("Invoice Number")
        or row.get("Invoice number")
        or row.get("Invoice")
    ).upper()


def key_from_attach(row: Dict[str, Any]) -> str:
    """Key for the Attachments master – should align with invoices key."""
    return norm(
        row.get("Invoice ID")
        or row.get("InvoiceID")
        or row.get("Xero number")
        or row.get("Invoice Number")
        or row.get("Invoice number")
        or row.get("Invoice")
    ).upper()


def key_from_issues(row: Dict[str, Any]) -> str:
    """Key for the Issues master – usually Invoice ID / Xero number."""
    return norm(
        row.get("Invoice ID")
        or row.get("InvoiceID")
        or row.get("Xero number")
        or row.get("Invoice Number")
        or row.get("Invoice number")
        or row.get("Invoice")
    ).upper()


# ---------- enrichment logic ----------

def build_enriched(
    invoices: List[Dict[str, Any]],
    attachments: List[Dict[str, Any]],
    issues: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], Set[str], Set[str]]:
    """Return enriched invoice rows plus sets of attachment & issues keys."""

    attach_map: Dict[str, List[Dict[str, Any]]] = {}
    issue_map: Dict[str, List[Dict[str, Any]]] = {}

    # Index attachments by invoice key
    for r in attachments:
        k = key_from_attach(r)
        if not k:
            continue
        attach_map.setdefault(k, []).append(r)

    # Index issues by invoice key
    for r in issues:
        k = key_from_issues(r)
        if not k:
            continue
        issue_map.setdefault(k, []).append(r)

    attach_keys = set(attach_map.keys())
    issues_keys = set(issue_map.keys())

    enriched_rows: List[Dict[str, Any]] = []

    for inv in invoices:
        inv_key = key_from_invoice(inv)
        a_rows = attach_map.get(inv_key, [])
        i_rows = issue_map.get(inv_key, [])

        has_attach = bool(a_rows)
        has_issues = bool(i_rows)

        # Attachment metadata (filenames etc.)
        filenames: List[str] = []
        for ar in a_rows:
            fn = (
                ar.get("File name")
                or ar.get("Filename")
                or ar.get("Name")
                or ar.get("Attachment")
            )
            fn_norm = norm(fn)
            if fn_norm and fn_norm not in filenames:
                filenames.append(fn_norm)
        sample_filenames = "; ".join(filenames[:5])

        # Issue flags / notes
        issue_flags: List[str] = []
        issue_notes: List[str] = []

        for ir in i_rows:
            flag = (
                ir.get("Reviewer decision")
                or ir.get("Reviewer Decision")
                or ir.get("Issue Flag")
                or ir.get("Issue_Flag")
                or ir.get("Issue")
                or ir.get("Untracked reason")
            )
            note = (
                ir.get("Reviewer notes")
                or ir.get("Reviewer Notes")
                or ir.get("Notes")
            )
            flag_norm = norm(flag)
            note_norm = norm(note)

            if flag_norm and flag_norm not in issue_flags:
                issue_flags.append(flag_norm)
            if note_norm and note_norm not in issue_notes:
                issue_notes.append(note_norm)

        flags_joined = "; ".join(issue_flags[:5])
        notes_joined = " | ".join(issue_notes[:5])

        out = dict(inv)  # copy original invoice row
        out["Has_Attachments"] = "Y" if has_attach else "N"
        out["Attachment_Count"] = len(a_rows)
        out["Attachment_Filenames_Sample"] = sample_filenames

        out["Has_Issues"] = "Y" if has_issues else "N"
        out["Issue_Count"] = len(i_rows)
        out["Issue_Flags"] = flags_joined
        out["Issue_Notes_Sample"] = notes_joined

        enriched_rows.append(out)

    return enriched_rows, attach_keys, issues_keys


# ---------- Apify entrypoint ----------

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
            Actor.log.error("Missing 'InvoicesUrl' – cannot build enriched master without base invoices.")
            return

        # Download CSVs
        invoices_rows = download_csv(invoices_url, "invoices")
        attach_rows: List[Dict[str, Any]] = []
        issues_rows: List[Dict[str, Any]] = []

        if attach_url:
            attach_rows = download_csv(attach_url, "attachments")
        else:
            Actor.log.warning("No AttachUrl provided; skipping attachments.")

        if issues_url:
            issues_rows = download_csv(issues_url, "issues")
        else:
            Actor.log.warning("No IssuesUrl provided; skipping issues.")

        Actor.log.info(
            f"Row counts: invoices={len(invoices_rows)}, "
            f"attachments={len(attach_rows)}, issues={len(issues_rows)}"
        )

        if not invoices_rows:
            Actor.log.error("Invoices CSV is empty or failed to parse; aborting.")
            return

        # Enrich
        enriched_rows, attach_keys, issues_keys = build_enriched(
            invoices_rows, attach_rows, issues_rows
        )

        Actor.log.info(
            f"Key coverage: attach_keys={len(attach_keys)}, "
            f"issues_keys={len(issues_keys)}"
        )
        Actor.log.info(f"Enriched rows: {len(enriched_rows)}")

        # Determine CSV headers: original invoice headers + new fields
        base_fields = list(invoices_rows[0].keys())
        extra_fields = [
            "Has_Attachments",
            "Attachment_Count",
            "Attachment_Filenames_Sample",
            "Has_Issues",
            "Issue_Count",
            "Issue_Flags",
            "Issue_Notes_Sample",
        ]
        for f in extra_fields:
            if f not in base_fields:
                base_fields.append(f)

        filename = f"invoice_master_enriched_{year}.csv"

        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=base_fields, extrasaction="ignore")
        writer.writeheader()
        for row in enriched_rows:
            writer.writerow(row)

        csv_data = buf.getvalue()

        await Actor.set_value(
            filename,
            csv_data,
            content_type="text/csv; charset=utf-8",
        )

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
    Actor.run(main)
