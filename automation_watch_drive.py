"""
Full flow: Pick all PDFs from Google Drive (or local folder) -> OCR -> DB -> S3 -> move to Processed.

Supports multiple Google Drive sites via sites.json:
  [{"name": "Kalyani", "enabled": true, "google_drive_folder_id": "...", "google_service_account_json": "..."}]
Each site uses its own Drive folder and service account. The DB table is the same — Inv_ID is
determined automatically from the invoice content (pertain_to field).

Run once: processes every PDF across all enabled sites, then exits.
  Fallback: .env GOOGLE_DRIVE_FOLDER_ID + GOOGLE_SERVICE_ACCOUNT_JSON (single-site mode)
  Or: WATCH_DRIVE=<local path> for local folder.
"""
import json
import os
import sys
import tempfile
import traceback
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env")

from extract_invoice import extract_invoice
from db_client import insert_bill_process, insert_bill_photo
from s3_client import upload_pdf_to_s3, is_s3_configured, get_s3_object_url

BASE_DIR = Path(__file__).resolve().parent
EXTRACTED_DIR = BASE_DIR / "extracted"
SITES_JSON = BASE_DIR / "sites.json"
DEFAULT_WATCH = Path("G:/") if os.name == "nt" else Path("/mnt/g")


def load_all_sites() -> list[dict]:
    """Load ALL configured sites from sites.json (regardless of enabled flag).
    Returns empty list if file missing."""
    if not SITES_JSON.is_file():
        return []
    try:
        sites = json.loads(SITES_JSON.read_text(encoding="utf-8"))
        return [s for s in sites
                if s.get("google_drive_folder_id", "").strip()
                and s.get("google_service_account_json", "").strip()]
    except (json.JSONDecodeError, TypeError) as e:
        print(f"[WARNING] Could not parse sites.json: {e}")
        return []


def load_sites(filter_names: list[str] | None = None) -> list[dict]:
    """Load sites from sites.json.
    - If filter_names is given, return only those sites (by name, case-insensitive), ignoring enabled flag.
    - Otherwise return only enabled sites."""
    all_sites = load_all_sites()
    if filter_names:
        requested = {n.lower() for n in filter_names}
        matched = [s for s in all_sites if s.get("name", "").lower() in requested]
        # Warn about names that didn't match any site
        found_names = {s.get("name", "").lower() for s in matched}
        for name in filter_names:
            if name.lower() not in found_names:
                print(f"[WARNING] Site '{name}' not found in sites.json. Available: {', '.join(s.get('name', '?') for s in all_sites)}")
        return matched
    return [s for s in all_sites if s.get("enabled", True)]


def use_google_drive() -> bool:
    # Multi-site mode: sites.json has enabled entries
    if load_sites():
        return True
    # Single-site fallback: .env vars
    return bool(
        os.environ.get("GOOGLE_DRIVE_FOLDER_ID", "").strip()
        and os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "").strip()
    )


def get_process_limit() -> int:
    """0 = no limit (process all). Set PROCESS_LIMIT=3 in .env to process only first 3 PDFs (for testing)."""
    raw = (os.environ.get("PROCESS_LIMIT") or "0").strip()
    try:
        n = int(raw)
        return max(0, n)
    except ValueError:
        return 0


def get_watch_folder() -> Path:
    raw = os.environ.get("WATCH_DRIVE", "").strip()
    return Path(raw) if raw else DEFAULT_WATCH


def run_one_pdf(
    pdf_path: Path,
    created_by: int = 0,
    json_stem: str | None = None,
    original_pdf_name: str | None = None,
):
    """Pick one PDF: OCR -> DB -> S3 -> T_Bill_Photo. Returns (success, s3_key, bill_process_id).
    json_stem: name for saved JSON (e.g. original Drive filename without .pdf).
    original_pdf_name: filename used in S3 key (Bill_Images/<ts>~<name>); if None, uses pdf_path.name."""
    data = extract_invoice(pdf_path)
    EXTRACTED_DIR.mkdir(exist_ok=True)
    stem = (json_stem or pdf_path.stem).strip()
    if not stem:
        stem = pdf_path.stem
    json_path = EXTRACTED_DIR / f"{stem}.json"
    json_path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")

    _, bill_process_id = insert_bill_process(data, created_by=created_by)
    s3_key = upload_pdf_to_s3(pdf_path, original_filename=original_pdf_name)
    photo_name = (original_pdf_name or pdf_path.name).strip() or pdf_path.name
    if s3_key and bill_process_id:
        insert_bill_photo(bill_process_id, photo_name, s3_key, created_by=created_by)
    return True, s3_key, bill_process_id


def log_failed(base_dir: Path, pdf_name: str, error_message: str) -> None:
    """Append failed PDF and reason to failed_processing.log."""
    from datetime import datetime
    log_path = base_dir / "failed_processing.log"
    line = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | {pdf_name} | {error_message}\n"
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(line)


def process_google_drive(
    folder_id: str | None = None,
    service_account_json: str | None = None,
    site_name: str | None = None,
) -> int:
    """Process all PDFs from a single Google Drive folder.
    If folder_id/service_account_json are provided, use them directly (multi-site mode).
    Otherwise fall back to .env vars (single-site backward compat)."""
    from google_drive_client import (
        get_drive_service,
        get_or_create_processed_folder,
        get_folder_id_by_path,
        find_folder_by_name,
        list_pdf_files_in_folder,
        download_file,
        move_file_to_folder,
    )

    folder_id = (folder_id or os.environ.get("GOOGLE_DRIVE_FOLDER_ID", "")).strip()
    if not folder_id:
        print("[ERROR] GOOGLE_DRIVE_FOLDER_ID is not set in .env")
        return 0

    subfolder_path = os.environ.get("GOOGLE_DRIVE_SUBFOLDER", "").strip().strip('"').strip("'")
    target_folder_name = os.environ.get("GOOGLE_DRIVE_TARGET_FOLDER_NAME", "").strip().strip('"').strip("'")

    label = f" [{site_name}]" if site_name else ""
    print(f"[INFO]{label} Google Drive connected. Fetching PDF list...")
    try:
        service = get_drive_service(service_account_json)
    except Exception as e:
        print(f"[ERROR]{label} Failed to connect to Google Drive: {e}")
        traceback.print_exc()
        return 0

    target_folder_id = folder_id
    if target_folder_name:
        # Find folder by name anywhere under root (e.g. "03 MAR" nested under 2026, etc.)
        found_id = find_folder_by_name(service, folder_id, target_folder_name)
        if found_id:
            target_folder_id = found_id
            print(f"[INFO] Using folder by name: {target_folder_name!r} (id: {target_folder_id})")
        else:
            print(f"[WARNING] Folder named {target_folder_name!r} not found under root. Listing from root folder instead.")
            print("[INFO] To pick the 16 PDFs from '03 MAR': share that folder with the service account,")
            print("       then set GOOGLE_DRIVE_FOLDER_ID to that folder's ID (from its URL) and clear GOOGLE_DRIVE_TARGET_FOLDER_NAME.")
    elif subfolder_path:
        target_folder_id = get_folder_id_by_path(service, folder_id, subfolder_path)
        if not target_folder_id:
            print(f"[ERROR] Subfolder not found: {subfolder_path!r} under GOOGLE_DRIVE_FOLDER_ID")
            return 0
        print(f"[INFO] Using subfolder: {subfolder_path!r} (folder id: {target_folder_id})")

    try:
        files = list_pdf_files_in_folder(service, target_folder_id, recursive=True)
    except Exception as e:
        err_str = str(e)
        if "404" in err_str or "not found" in err_str.lower() or "notFound" in err_str:
            print("[ERROR] Folder not found (404). To list PDFs from a specific folder (e.g. 03 MAR with 16 PDFs):")
            print("  1. Open that folder in Google Drive and copy the folder ID from the URL (.../folders/FOLDER_ID).")
            print("  2. Share that folder with your service account email (see the JSON key file).")
            print("  3. Set GOOGLE_DRIVE_FOLDER_ID in .env to that folder ID.")
            raise
        raise
    pdfs = [f for f in files if (f.get("name") or "").lower().endswith(".pdf")]
    if not pdfs:
        print("[INFO] No PDFs found in the Drive folder.")
        return 0

    limit = get_process_limit()
    total_in_folder = len(pdfs)
    if limit > 0:
        pdfs = pdfs[:limit]
        print(f"[INFO] Found {total_in_folder} PDF(s) in folder. PROCESS_LIMIT={limit} -> processing first {len(pdfs)} only (for testing).")
    else:
        print(f"[INFO] Found {len(pdfs)} PDF(s). Processing one by one.")
    if len(pdfs) > 0:
        print("[INFO] (Only PDFs in this folder are listed; then processed one by one.)")
    print("-" * 60)
    processed_folder_id = get_or_create_processed_folder(service, target_folder_id)
    done = 0

    for i, f in enumerate(pdfs, 1):
        file_id = f["id"]
        name = f.get("name") or "unknown.pdf"
        print(f"[{i}/{len(pdfs)}] {name}")

        try:
            print("  -> Downloading from Drive...")
            with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
                tmp_path = Path(tmp.name)
            try:
                download_file(service, file_id, tmp_path)

                print("  -> Extracting (OCR)...")
                original_stem = Path(name).stem if name else None
                _, s3_key, bill_process_id = run_one_pdf(
                    tmp_path, created_by=0, json_stem=original_stem, original_pdf_name=name
                )

                s3_msg = f", S3: {s3_key}" if s3_key else ""
                if s3_key:
                    s3_url = get_s3_object_url(s3_key)
                    if s3_url:
                        print(f"  -> S3 URL: {s3_url}")
                print(f"  -> DB + S3 updated. Bill_Process_ID: {bill_process_id}{s3_msg} (verify in DB, then moving.)")
                print("  -> Moving to Processed folder in Drive...")
                try:
                    parent_id = f.get("parent_id") or folder_id
                    move_file_to_folder(service, file_id, processed_folder_id, parent_id)
                except Exception as move_err:
                    err_str = str(move_err)
                    if "403" in err_str or "write access" in err_str.lower() or "appNotAuthorizedToFile" in err_str:
                        print(f"  [WARNING] Processed OK but could not move file (grant Editor to service account on folder).")
                    else:
                        raise

                print(f"  [OK] Done. Moved to Processed. Bill_Process_ID: {bill_process_id}{s3_msg}")
                done += 1
            finally:
                if tmp_path.exists():
                    try:
                        tmp_path.unlink()
                    except OSError:
                        pass
        except Exception as e:
            print(f"  [ERROR] {e}")
            log_failed(BASE_DIR, name, str(e))
            traceback.print_exc()

    return done


def process_local_inbox(watch_dir: Path) -> int:
    watch_dir.mkdir(parents=True, exist_ok=True)
    (watch_dir / "processed").mkdir(exist_ok=True)
    pdf_paths = sorted(watch_dir.glob("*.pdf"))
    if not pdf_paths:
        print("[INFO] No PDFs found in folder.")
        return 0

    limit = get_process_limit()
    total_in_folder = len(pdf_paths)
    if limit > 0:
        pdf_paths = pdf_paths[:limit]
        print(f"[INFO] Found {total_in_folder} PDF(s) in folder. PROCESS_LIMIT={limit} -> processing first {len(pdf_paths)} only (for testing).")
    else:
        print(f"[INFO] Found {len(pdf_paths)} PDF(s). Processing one by one.")
    print("-" * 60)
    done = 0
    for i, pdf_path in enumerate(pdf_paths, 1):
        print(f"[{i}/{len(pdf_paths)}] {pdf_path.name}")
        try:
            print("  -> Extracting (OCR)...")
            _, s3_key, bill_process_id = run_one_pdf(pdf_path)
            dest = watch_dir / "processed" / pdf_path.name
            if dest.exists():
                dest.unlink()
            pdf_path.rename(dest)
            s3_msg = f", S3: {s3_key}" if s3_key else ""
            if s3_key:
                s3_url = get_s3_object_url(s3_key)
                if s3_url:
                    print(f"  -> S3 URL: {s3_url}")
            print(f"  [OK] Done. Bill_Process_ID: {bill_process_id}, Moved to processed/{s3_msg}")
            done += 1
        except Exception as e:
            print(f"  [ERROR] {e}")
            log_failed(BASE_DIR, pdf_path.name, str(e))
            traceback.print_exc()

    return done


def main():
    # Parse command-line args: site names to process (or --list to show available sites)
    args = [a.strip() for a in sys.argv[1:] if a.strip()]

    # --list / --help: show available sites and exit
    if any(a.lower() in ("--list", "--sites", "--help", "-h") for a in args):
        all_sites = load_all_sites()
        if all_sites:
            print("Available sites in sites.json:")
            for s in all_sites:
                status = "enabled" if s.get("enabled", True) else "disabled"
                print(f"  - {s['name']} ({status})")
            print()
            print("Usage:")
            print("  python automation_watch_drive.py                  # process all enabled sites")
            print("  python automation_watch_drive.py kalyani          # process only Kalyani")
            print("  python automation_watch_drive.py falta ho         # process Falta and HO")
            print("  python automation_watch_drive.py kalyani falta ho # process all three")
        else:
            print("No sites configured. Add sites to sites.json or set GOOGLE_DRIVE_FOLDER_ID in .env")
        sys.exit(0)

    # Filter sites by command-line args (if given)
    site_filter = args if args else None

    use_drive = use_google_drive()
    s3_status = "S3 upload enabled" if is_s3_configured() else "S3 not configured (skipped)"

    if use_drive:
        sites = load_sites(filter_names=site_filter)
        total = 0

        if sites:
            site_label = ", ".join(s["name"] for s in sites)
            if site_filter:
                print("=" * 60)
                print(f"Source: Google Drive — processing requested site(s): {site_label}")
            else:
                print("=" * 60)
                print(f"Source: Google Drive — {len(sites)} enabled site(s): {site_label}")
            print(f"Flow: Pick PDFs -> OCR -> T_Bill_Process + T_Bill_HSN_SAC + T_Bill_Photo -> {s3_status} -> Move to Processed")
            print("=" * 60)

            for site in sites:
                name = site.get("name", "Unknown")
                print()
                print(f"{'=' * 60}")
                print(f"  Processing site: {name}")
                print(f"{'=' * 60}")
                n = process_google_drive(
                    folder_id=site["google_drive_folder_id"],
                    service_account_json=site["google_service_account_json"],
                    site_name=name,
                )
                total += n
                print(f"[INFO] [{name}] Done. {n} PDF(s) processed.")
        elif site_filter:
            # User asked for specific site(s) but none matched
            print(f"[ERROR] No matching sites found for: {', '.join(site_filter)}")
            print("Run with --list to see available sites.")
            sys.exit(1)
        else:
            # No sites.json entries, fallback to .env
            print("=" * 60)
            print("Source: Google Drive (GOOGLE_DRIVE_FOLDER_ID from .env)")
            print(f"Flow: Pick PDFs -> OCR -> T_Bill_Process + T_Bill_HSN_SAC + T_Bill_Photo -> {s3_status} -> Move to Processed")
            print("=" * 60)
            total = process_google_drive()

        print("-" * 60)
        print(f"[INFO] Finished. {total} PDF(s) processed successfully.")
    else:
        watch_dir = get_watch_folder()
        print("=" * 60)
        print(f"Source: Local folder ({watch_dir})")
        print(f"Flow: Pick PDFs -> OCR -> DB -> {s3_status} -> Move to processed/")
        print("=" * 60)
        n = process_local_inbox(watch_dir)
        print("-" * 60)
        print(f"[INFO] Finished. {n} PDF(s) processed successfully.")

    sys.exit(0)


if __name__ == "__main__":
    main()
