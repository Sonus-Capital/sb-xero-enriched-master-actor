import csv
import io
import json
import urllib.request
from typing import List, Dict, Any, Tuple, DefaultDict
from collections import defaultdict

from apify import Actor


# ----------------- small helpers -----------------


def norm(s: Any) -> str:
    if s is None:
        return ""
    return str(s).strip()


def download_csv(url: str, label: str) -> List[Dict[str, Any]]:
    """Download a CSV from Dropbox and parse it into a list of dicts.

    First try csv.DictReader normally.
    If that fails (bad newlines etc.), fall back to a naive splitter.
    """
    if not url:
        return []

    Actor.log.info(f"Downloading {label} CSV from {url}")

    with urllib.request.urlopen(url) as resp:
        csv_bytes = resp.read()

    text = csv_bytes.decode("utf-8", errors="replace")

    rows: List[Dict[str, Any]] = []

    # First attempt: proper CSV parsing
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


# --- key extractors (robust to header differences) ---


def invoice_key_from_invoice(row: Dict[str, Any]) -> str:
    """Key for main invoice master rows."""
    return norm(
        row.get("Invoice ID")
        or row.get("Invoice Id")
        or row.get("InvoiceID")
        or row.get("Xero number")
        or row.get("Invoice Number")
        or row.get("Invoice number")
        or row.get("Invoice")
    ).upper()


def invoice_key_from_attach(row: Dict[str, Any]) -> str:
    """Key for attachment master rows."""
    return norm(
        row.get("Invoice ID")
        or row.get("Invoice Id")
        or row.get("InvoiceID")
        or row.get("Xero number")
        or row.get("Invoice Number")
        or row.get("Invoice number")
        or row.get("Invoice")
    ).upper()


def invoice_key_from_issues(row: Dict[str, Any]) -> str:
    """Key for issues master rows."""
    return norm(
        row.get("Invoice ID")
        or row.get("Invoice Id")
        or row.get("InvoiceID")
        or row.get("Xero number")
        or row.get("Invoice Number")
        or row.get("Invoice number")
        or row.get("Invoice")
    ).upper()


# --- simple feature extractors for attachments / issues ---


def attachment_features(rows: List[Dict[str, Any]]) -> Dict[str, str]:
    """Aggregate attachment rows for one invoice into summary fields."""
    if not rows:
        return {
            "Attach_Count": "0",
            "Attach_Doc_IDs": "",
            "Attach_Sources": "",
            "Attach_Filenames": "",
        }

    doc_ids = set()
    sources = set()
    filenames = set()

    for r in rows:
        doc_ids.add(norm(r.get("Doc ID") or r.get("DocId") or r.get("Document ID")))
        sources.add(norm(r.get("Source Doc") or r.get("SourceDoc") or r.get("Source")))
        filenames.add(norm(r.get("File Name") or r.get("Filename") or r.get("Name")))

    # strip empties
    doc_ids.discard("")
    sources.discard("")
    filenames.discard("")

    return {
        "Attach_Count": str(len(rows)),
        "Attach_Doc_IDs": "; ".join(sorted(doc_ids)) if doc_ids else "",
        "Attach_Sources": "; ".join(sorted(s for s in sources if s)) if sources else "",
        "Attach_Filenames": "; ".join(sorted(f for f in filenames if f)) if filenames else "",
    }


def issue_features(rows: List[Dict[str, Any]]) -> Dict[str, str]:
    """Aggregate issue rows for one invoice into summary fields."""
    if not rows:
        return {
            "Has_Issue": "N",
            "Issue_Flags": "",
            "Issue_Untracked_Flags": "",
            "Issue_Reviewer_Decisions": "",
        }

    flags = set()
    untracked_flags = set()
    reviewer_decisions = set()

    for r in rows:
        # generic 'Issue_Flag' style
        flags.add(norm(r.get("Issue_Flag") or r.get("Issue Flag")))
        # from your earlier schemas
        untracked_flags.add(norm(r.get("Untracked flag") or r.get("Untracked Flag")))
        reviewer_decisions.add(norm(r.get("Reviewer decision") or r.get("Reviewer Decision")))

    flags.discard("")
    untracked_flags.discard("")
    reviewer_decisions.discard("")

    return {
        "Has_Issue": "Y",
        "Issue_Flags": "; ".join(sorted(flags)) if flags else "",
        "Issue_Untracked_Flags": "; ".join(sorted(untracked_flags)) if untracked_flags else "",
        "Issue_Reviewer_Decisions": "; ".join(sorted(reviewer_decisions)) if reviewer_decisions else "",
    }


def build_enriched(
    invoice_rows: List[Dict[str, Any]],
    attach_rows: List[Dict[str, Any]],
    issue_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Return enriched invoice rows by joining attachments + issues on invoice key."""
    # Index attachments & issues
    attach_by_key: DefaultDict[str, List[Dict[str, Any]]] = defaultdict(list)
    issue_by_key: DefaultDict[str, List[Dict[str, Any]]] = defaultdict(list)

    for r in attach_rows:
        k = invoice_key_from_attach(r)
        if not k:
            continue
        attach_by_key[k].append(r)

    for r in issue_rows:
        k = invoice_key_from_issues(r)
        if not k:
            continue
        issue_by_key[k].append(r)

    Actor.log.info(
        f"Index sizes: attach_keys={len(attach_by_key)}, "
        f"issue_keys={len(issue_by_key)}"
    )

    enriched: List[Dict[str, Any]] = []

    for base in invoice_rows:
        key = invoice_key_from_invoice(base)
        if not key:
            # keep but mark as having no key so you can fix later if needed
            key = ""

        a_feat = attachment_features(attach_by_key.get(key, []))
        i_rows = issue_by_key.get(key, [])
        if i_rows:
            i_feat = issue_features(i_rows)
        else:
            i_feat = {
                "Has_Issue": "N",
                "Issue_Flags": "",
                "Issue_Untracked_Flags": "",
                "Issue_Reviewer_Decisions": "",
            }

        row_out = dict(base)
        # make sure we always have the normalized key column as well
        row_out["Invoice_Key_Normalized"] = key
        row_out.update(a_feat)
        row_out.update(i_feat)
        enriched.append(row_out)

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
            Actor.log.error("Missing 'InvoicesUrl' (master invoices CSV).")
            return

        if not (attach_url or issues_url):
            Actor.log.warning(
                "No AttachUrl / IssuesUrl provided. "
                "Will still run, but enriched fields may be empty."
            )

        # 1) Download CSVs
        invoice_rows = download_csv(invoices_url, "invoices")
        attach_rows: List[Dict[str, Any]] = []
        issue_rows: List[Dict[str, Any]] = []

        if attach_url:
            attach_rows = download_csv(attach_url, "attachments")
        if issues_url:
            issue_rows = download_csv(issues_url, "issues")

        if not invoice_rows:
            Actor.log.error("Invoice CSV is empty or failed to parse; aborting.")
            return

        Actor.log.info(
            f"Row counts: invoices={len(invoice_rows)}, "
            f"attachments={len(attach_rows)}, issues={len(issue_rows)}"
        )

        # 2) Build enriched rows
        enriched_rows = build_enriched(invoice_rows, attach_rows, issue_rows)
        Actor.log.info(f"Enriched rows: {len(enriched_rows)}")

        # 3) Write CSV to KV store
        filename = f"invoice_enriched_{year}.csv"

        # Collect all fieldnames dynamically (base + new columns)
        fieldnames_set = set()
        for r in enriched_rows:
            fieldnames_set.update(r.keys())
        fieldnames = sorted(fieldnames_set)

        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for r in enriched_rows:
            writer.writerow(r)

        csv_data = buf.getvalue()

        await Actor.set_value(
            filename,
            csv_data,
            content_type="text/csv; charset=utf-8",
        )

        # 4) Push a small JSON summary row so Make's "Get dataset" has something
        summary = {
            "year": year,
            "invoice_rows": len(invoice_rows),
            "attachment_rows": len(attach_rows),
            "issue_rows": len(issue_rows),
            "enriched_rows": len(enriched_rows),
            "kv_filename": filename,
        }
        await Actor.push_data(summary)

        Actor.log.info(
            f"Done. Year={year}, enriched_rows={len(enriched_rows)}, "
            f"kv_file={filename}"
        )
