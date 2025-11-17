import csv
import io
import json
import urllib.request
from typing import List, Dict, Any, Tuple, Set

from apify import Actor


# ----------------- small helpers -----------------


def norm(value: Any) -> str:
    """Normalize a field to a stripped string, or empty string for None."""
    if value is None:
        return ""
    return str(value).strip()


def download_csv(url: str, label: str) -> List[Dict[str, Any]]:
    """Download a CSV from Dropbox and parse it into a list of dicts.

    Primary attempt: csv.DictReader.
    Fallback: simple split-based parser if DictReader chokes (bad newlines, etc.).
    """
    if not url:
        return []

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

    # Fallback parser
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


# ----------------- key functions -----------------


def invoice_key_from_invoices(row: Dict[str, Any]) -> str:
    """Key function for the invoice master (2016_Master_Invoices…)."""
    return (
        norm(
            row.get("Invoice ID")
            or row.get("InvoiceId")
            or row.get("InvoiceID")
            or row.get("Xero number")
            or row.get("Xero Number")
            or row.get("Invoice Number")
            or row.get("Invoice number")
            or row.get("Invoice")
            or row.get("Key")
        )
        .upper()
    )


def invoice_key_from_attachments(row: Dict[str, Any]) -> str:
    """Key function for the attachment master (2016_Master_Attachments…)."""
    return (
        norm(
            row.get("Invoice ID")
            or row.get("InvoiceId")
            or row.get("InvoiceID")
            or row.get("Xero number")
            or row.get("Xero Number")
            or row.get("Invoice Number")
            or row.get("Invoice number")
            or row.get("Invoice")
            or row.get("Key")
        )
        .upper()
    )


def invoice_key_from_issues(row: Dict[str, Any]) -> str:
    """Key function for the issues master (2016_Master_Issues…)."""
    return (
        norm(
            row.get("Invoice ID")
            or row.get("InvoiceId")
            or row.get("InvoiceID")
            or row.get("Xero number")
            or row.get("Xero Number")
            or row.get("Invoice Number")
            or row.get("Invoice number")
            or row.get("Invoice")
            or row.get("Key")
        )
        .upper()
    )


# ----------------- enrichment logic -----------------


def build_enriched_rows(
    invoices: List[Dict[str, Any]],
    attachments: List[Dict[str, Any]],
    issues: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], Set[str], Set[str]]:
    """Return the enriched invoice rows, and the sets of attachment and issue keys used."""

    attach_map: Dict[str, List[Dict[str, Any]]] = {}
    issues_map: Dict[str, List[Dict[str, Any]]] = {}

    # Index attachments
    for row in attachments:
        k = invoice_key_from_attachments(row)
        if not k:
            continue
        attach_map.setdefault(k, []).append(row)

    # Index issues
    for row in issues:
        k = invoice_key_from_issues(row)
        if not k:
            continue
        issues_map.setdefault(k, []).append(row)

    attach_keys = set(attach_map.keys())
    issue_keys = set(issues_map.keys())

    enriched: List[Dict[str, Any]] = []

    for inv in invoices:
        inv_key = invoice_key_from_invoices(inv)
        inv_key_upper = inv_key.upper()

        inv_attach_rows = attach_map.get(inv_key_upper, [])
        inv_issue_rows = issues_map.get(inv_key_upper, [])

        # Attachments: collect Doc IDs and Source Docs if present
        doc_ids = sorted(
            {
                norm(
                    r.get("Doc ID")
                    or r.get("DocId")
                    or r.get("Document ID")
                    or r.get("Attachment ID")
                    or r.get("AttachmentId")
                )
                for r in inv_attach_rows
                if norm(
                    r.get("Doc ID")
                    or r.get("DocId")
                    or r.get("Document ID")
                    or r.get("Attachment ID")
                    or r.get("AttachmentId")
                )
            }
        )

        source_docs = sorted(
            {
                norm(r.get("Source Doc") or r.get("Source_Doc") or r.get("Source"))
                for r in inv_attach_rows
                if norm(r.get("Source Doc") or r.get("Source_Doc") or r.get("Source"))
            }
        )

        # Issues: collect flags/descriptions if present
        issue_flags = sorted(
            {
                norm(
                    r.get("Issue Flag")
                    or r.get("Issue_Flag")
                    or r.get("Issue")
                    or r.get("Issue code")
                    or r.get("Issue_code")
                    or r.get("Untracked reason")
                    or r.get("Reviewer decision")
                )
                for r in inv_issue_rows
                if norm(
                    r.get("Issue Flag")
                    or r.get("Issue_Flag")
                    or r.get("Issue")
                    or r.get("Issue code")
                    or r.get("Issue_code")
                    or r.get("Untracked reason")
                    or r.get("Reviewer decision")
                )
            }
        )

        out_row = dict(inv)  # start with invoice master fields

        # Enrichment fields
        out_row["Enriched_Attachment_Count"] = str(len(inv_attach_rows))
        out_row["Enriched_Attachment_Doc_IDs"] = "; ".join(doc_ids) if doc_ids else ""
        out_row["Enriched_Attachment_Source_Docs"] = (
            "; ".join(source_docs) if source_docs else ""
        )
        out_row["Enriched_Issue_Count"] = str(len(inv_issue_rows))
        out_row["Enriched_Issue_Flags"] = "; ".join(issue_flags) if issue_flags else ""
        out_row["Enriched_Invoice_Key"] = inv_key_upper

        enriched.append(out_row)

    return enriched, attach_keys, issue_keys


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
        invoices_rows = download_csv(invoices_url, "invoices")
        attachments_rows: List[Dict[str, Any]] = []
        issues_rows: List[Dict[str, Any]] = []

        if attach_url:
            attachments_rows = download_csv(attach_url, "attachments")
        else:
            Actor.log.warning("No AttachUrl provided; skipping attachment enrichment.")

        if issues_url:
            issues_rows = download_csv(issues_url, "issues")
        else:
            Actor.log.warning("No IssuesUrl provided; skipping issues enrichment.")

        if not invoices_rows:
            Actor.log.error("Invoices CSV is empty or failed to parse; aborting.")
            return

        Actor.log.info(
            f"Row counts: invoices={len(invoices_rows)}, "
            f"attachments={len(attachments_rows)}, issues={len(issues_rows)}"
        )

        # 2) Build enriched rows
        enriched_rows, attach_keys, issue_keys = build_enriched_rows(
            invoices_rows, attachments_rows, issues_rows
        )

        Actor.log.info(
            f"Key coverage: attach_keys={len(attach_keys)}, "
            f"issues_keys={len(issue_keys)}"
        )
        Actor.log.info(f"Enriched rows: {len(enriched_rows)}")

        # 3) Write enriched CSV to KV store
        filename = f"invoice_master_enriched_{year}.csv"

        # Preserve original invoice columns order, then append enrichment columns
        invoice_fieldnames = list(invoices_rows[0].keys())
        enrichment_fields = [
            "Enriched_Attachment_Count",
            "Enriched_Attachment_Doc_IDs",
            "Enriched_Attachment_Source_Docs",
            "Enriched_Issue_Count",
            "Enriched_Issue_Flags",
            "Enriched_Invoice_Key",
        ]
        fieldnames = invoice_fieldnames + [
            f for f in enrichment_fields if f not in invoice_fieldnames
        ]

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

        # 4) Push a small JSON summary to dataset
        summary = {
            "year": year,
            "invoice_rows": len(invoices_rows),
            "attachment_rows": len(attachments_rows),
            "issue_rows": len(issues_rows),
            "enriched_rows": len(enriched_rows),
            "kv_filename": filename,
        }
        await Actor.push_data(summary)

        Actor.log.info(
            f"Done. Year={year}, enriched_rows={len(enriched_rows)}, kv_file={filename}"
        )


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
