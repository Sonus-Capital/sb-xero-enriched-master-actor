import asyncio
import csv
import io
import json
import urllib.request
from typing import Any, Dict, List, Tuple, Set

from apify import Actor


# ----------------- small helpers -----------------


def norm(s: Any) -> str:
    """Normalise a value to a trimmed string (or empty string)."""
    if s is None:
        return ""
    return str(s).strip()


def download_csv(url: str, label: str) -> Tuple[List[Dict[str, Any]], List[str]]:
    """Download a CSV from Dropbox and parse it into a list of dicts.

    Returns (rows, header_fieldnames).

    Primary attempt: csv.DictReader (normal).
    Fallback: very simple split-based parser if DictReader chokes (e.g. bad newlines).
    """
    if not url:
        return [], []

    Actor.log.info(f"Downloading {label} CSV from {url}")

    with urllib.request.urlopen(url) as resp:
        csv_bytes = resp.read()

    # Decode text
    text = csv_bytes.decode("utf-8", errors="replace")

    rows: List[Dict[str, Any]] = []
    header: List[str] = []

    # First try: normal DictReader
    try:
        sio = io.StringIO(text)
        reader = csv.DictReader(sio)
        header = reader.fieldnames or []
        rows = [dict(r) for r in reader]
        Actor.log.info(f"{label} rows (DictReader): {len(rows)}")
        return rows, header
    except csv.Error as e:
        Actor.log.warning(
            f"{label} CSV parse via DictReader failed: {e!r}. "
            f"Falling back to simple split parser; some rows may be skipped."
        )

    # Fallback: naive split-by-line, split-by-comma parser.
    lines = [ln for ln in text.splitlines() if ln.strip()]
    if not lines:
        Actor.log.warning(f"{label} CSV has no non-empty lines after fallback parsing.")
        return [], []

    header = [h.strip() for h in lines[0].split(",")]
    for ln in lines[1:]:
        parts = [p.strip() for p in ln.split(",")]
        if len(parts) != len(header):
            # Skip malformed lines in fallback mode
            continue
        row = dict(zip(header, parts))
        rows.append(row)

    Actor.log.info(f"{label} rows (fallback): {len(rows)}")
    return rows, header


# ----------------- key functions -----------------


def invoice_key_from_invoices(row: Dict[str, Any]) -> str:
    """Key function for the invoice master (2016_Master_Invoices etc.)."""
    return norm(
        row.get("Invoice ID")
        or row.get("InvoiceID")
        or row.get("Invoice Id")
        or row.get("Xero number")
        or row.get("Invoice Number")
        or row.get("Invoice")
    ).upper()


def invoice_key_from_attachments(row: Dict[str, Any]) -> str:
    """Key function for the attachments master (2016_Master_Attachments etc.)."""
    return norm(
        row.get("Invoice ID")
        or row.get("InvoiceID")
        or row.get("Invoice Id")
        or row.get("Xero number")
        or row.get("Invoice Number")
        or row.get("Invoice")
    ).upper()


def invoice_key_from_issues(row: Dict[str, Any]) -> str:
    """Key function for the issues master (2016_Master_Issues etc.)."""
    return norm(
        row.get("Invoice ID")
        or row.get("InvoiceID")
        or row.get("Invoice Id")
        or row.get("Key")  # fallback to your 'Key' column if present
    ).upper()


# ----------------- enrichment logic -----------------


def build_index(
    rows: List[Dict[str, Any]], key_fn
) -> Dict[str, List[Dict[str, Any]]]:
    index: Dict[str, List[Dict[str, Any]]] = {}
    for r in rows:
        k = key_fn(r)
        if not k:
            continue
        index.setdefault(k, []).append(r)
    return index


def extract_attachment_fields(attach_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Aggregate attachment metadata for a single invoice key."""
    doc_ids: Set[str] = set()
    file_names: Set[str] = set()
    paths: Set[str] = set()

    for r in attach_rows:
        doc_id = norm(
            r.get("Doc ID")
            or r.get("DocID")
            or r.get("Attachment ID")
            or r.get("AttachmentId")
            or r.get("Id")
        )
        if doc_id:
            doc_ids.add(doc_id)

        name = norm(
            r.get("name")
            or r.get("Name")
            or r.get("file_name")
            or r.get("File Name")
            or r.get("FileName")
        )
        if name:
            file_names.add(name)

        path = norm(
            r.get("path_lower")
            or r.get("Path_Lower")
            or r.get("dbx_path_lower")
            or r.get("Dropbox path lower")
            or r.get("path_display")
        )
        if path:
            paths.add(path)

    # Build Dropbox API download args for convenience
    download_args: List[str] = []
    for p in sorted(paths):
        if not p:
            continue
        # This is the string you plug into Dropbox-API-Arg header
        # e.g. {"path": "/sona/..."}
        download_args.append(json.dumps({"path": p}, separators=(",", ":")))

    return {
        "Enriched_Attachment_Count": len(attach_rows),
        "Enriched_Attachment_Doc_IDs": "; ".join(sorted(doc_ids)) if doc_ids else "",
        "Enriched_Attachment_File_Names": "; ".join(sorted(file_names)) if file_names else "",
        "Enriched_Attachment_Paths": "; ".join(sorted(paths)) if paths else "",
        "Enriched_Attachment_Download_Args": "; ".join(download_args) if download_args else "",
    }


def extract_issue_fields(issue_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Aggregate issue flags / notes for a single invoice key."""
    flags: Set[str] = set()

    for r in issue_rows:
        flag = norm(
            r.get("Issue_Flag")
            or r.get("Issue flag")
            or r.get("Issue Flag")
            or r.get("Issue")
            or r.get("Status")
        )
        if flag:
            flags.add(flag)

    return {
        "Enriched_Issue_Count": len(issue_rows),
        "Enriched_Issue_Flags": "; ".join(sorted(flags)) if flags else "",
    }


def build_enriched_rows(
    invoice_rows: List[Dict[str, Any]],
    attach_rows: List[Dict[str, Any]],
    issue_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Merge invoice, attachment, and issues data into enriched invoice rows."""
    attach_index = build_index(attach_rows, invoice_key_from_attachments)
    issue_index = build_index(issue_rows, invoice_key_from_issues)

    attach_keys = set(attach_index.keys())
    issue_keys = set(issue_index.keys())

    Actor.log.info(
        f"Key coverage: attach_keys={len(attach_keys)}, issues_keys={len(issue_keys)}"
    )

    enriched: List[Dict[str, Any]] = []

    for inv in invoice_rows:
        key = invoice_key_from_invoices(inv)
        inv_copy = dict(inv)  # do not mutate original

        a_rows = attach_index.get(key, [])
        i_rows = issue_index.get(key, [])

        # Attachments
        attach_fields = (
            extract_attachment_fields(a_rows) if a_rows else {
                "Enriched_Attachment_Count": 0,
                "Enriched_Attachment_Doc_IDs": "",
                "Enriched_Attachment_File_Names": "",
                "Enriched_Attachment_Paths": "",
                "Enriched_Attachment_Download_Args": "",
            }
        )

        # Issues
        issue_fields = (
            extract_issue_fields(i_rows) if i_rows else {
                "Enriched_Issue_Count": 0,
                "Enriched_Issue_Flags": "",
            }
        )

        inv_copy.update(attach_fields)
        inv_copy.update(issue_fields)

        enriched.append(inv_copy)

    Actor.log.info(f"Enriched rows: {len(enriched)}")
    return enriched


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
            Actor.log.error("Missing 'InvoicesUrl' in input; cannot build enriched master.")
            return

        # 1) Download CSVs
        invoice_rows, invoice_header = download_csv(invoices_url, "invoices")
        attach_rows: List[Dict[str, Any]] = []
        issues_rows: List[Dict[str, Any]] = []

        if attach_url:
            attach_rows, _ = download_csv(attach_url, "attachments")
        else:
            Actor.log.warning("No AttachUrl provided; skipping attachments.")

        if issues_url:
            issues_rows, _ = download_csv(issues_url, "issues")
        else:
            Actor.log.warning("No IssuesUrl provided; skipping issues.")

        Actor.log.info(
            f"Row counts: invoices={len(invoice_rows)}, "
            f"attachments={len(attach_rows)}, issues={len(issues_rows)}"
        )

        if not invoice_rows:
            Actor.log.error("Invoice CSV had no rows; aborting.")
            return

        # 2) Build enriched rows
        enriched_rows = build_enriched_rows(invoice_rows, attach_rows, issues_rows)

        # 3) Write CSV to KV store
        filename = f"invoice_master_enriched_{year}.csv"

        # Preserve original invoice column order and append enrichment columns
        base_fields = invoice_header or list(invoice_rows[0].keys())
        enrichment_fields = [
            "Enriched_Attachment_Count",
            "Enriched_Attachment_Doc_IDs",
            "Enriched_Attachment_File_Names",
            "Enriched_Attachment_Paths",
            "Enriched_Attachment_Download_Args",
            "Enriched_Issue_Count",
            "Enriched_Issue_Flags",
        ]
        fieldnames = base_fields + [f for f in enrichment_fields if f not in base_fields]

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

        # 4) Push a small JSON summary to dataset so Make "Get dataset" works
        summary = {
            "year": year,
            "invoice_rows": len(invoice_rows),
            "attachment_rows": len(attach_rows),
            "issue_rows": len(issues_rows),
            "enriched_rows": len(enriched_rows),
            "kv_filename": filename,
        }
        await Actor.push_data(summary)

        Actor.log.info(
            f"Done. Year={year}, enriched_rows={len(enriched_rows)}, kv_file={filename}"
        )


if __name__ == "__main__":
    asyncio.run(main())
