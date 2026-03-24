"""
Pipeline: PDF -> extract (Claude) -> T_Bill_Process + T_Bill_HSN_SAC -> S3 -> T_Bill_Photo.

Usage:
  python pipeline_pdf_to_db.py                    # process all PDFs in input/
  python pipeline_pdf_to_db.py path/to/file.pdf  # process single PDF

Set PROCESS_LIMIT=3 in .env to process only first 3 PDFs (for testing).
"""
import json
import os
import sys
import traceback
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env")


def get_process_limit() -> int:
    """0 = no limit. PROCESS_LIMIT=3 -> process first 3 only (for testing)."""
    raw = (os.environ.get("PROCESS_LIMIT") or "0").strip()
    try:
        return max(0, int(raw))
    except ValueError:
        return 0

from extract_invoice import extract_invoice
from db_client import insert_bill_process, insert_bill_photo
from s3_client import upload_pdf_to_s3, is_s3_configured, get_s3_object_url

BASE_DIR = Path(__file__).resolve().parent
INPUT_DIR = BASE_DIR / "input"
EXTRACTED_DIR = BASE_DIR / "extracted"


def run_one_pdf(pdf_path: Path, created_by: int = 0):
    """Extract -> JSON -> T_Bill_Process + T_Bill_HSN_SAC -> S3 -> T_Bill_Photo. Returns (success, s3_key, bill_process_id)."""
    data = extract_invoice(pdf_path)
    EXTRACTED_DIR.mkdir(exist_ok=True)
    json_path = EXTRACTED_DIR / f"{pdf_path.stem}.json"
    json_path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")

    _, bill_process_id = insert_bill_process(data, created_by=created_by)
    s3_key = upload_pdf_to_s3(pdf_path)
    if s3_key and bill_process_id:
        insert_bill_photo(bill_process_id, pdf_path.name, s3_key, created_by=created_by)
    return True, s3_key, bill_process_id


def log_failed(base_dir: Path, pdf_name: str, error_message: str) -> None:
    """Append failed PDF and reason to failed_processing.log."""
    from datetime import datetime
    log_path = base_dir / "failed_processing.log"
    line = f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | {pdf_name} | {error_message}\n"
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(line)


def main():
    s3_note = " + S3" if is_s3_configured() else ""

    if len(sys.argv) >= 2:
        single = Path(sys.argv[1]).resolve()
        if not single.is_file():
            print(f"[ERROR] File not found: {single}")
            sys.exit(1)
        if single.suffix.lower() != ".pdf":
            print(f"[ERROR] Not a PDF: {single}")
            sys.exit(1)
        print(f"[INFO] Pipeline: PDF -> extract -> DB{s3_note}")
        print(f"[INFO] File: {single.name}")
        print("-" * 60)
        try:
            _, s3_key, bill_process_id = run_one_pdf(single)
            data = json.loads((EXTRACTED_DIR / f"{single.stem}.json").read_text(encoding="utf-8"))
            line_count = len(data.get("line_items") or [])
            if s3_key:
                s3_url = get_s3_object_url(s3_key)
                if s3_url:
                    print(f"S3 URL: {s3_url}")
            msg = f"[OK] Bill_Process_ID: {bill_process_id}, T_Bill_Process: 1 row, T_Bill_HSN_SAC: {line_count} row(s)"
            if s3_key:
                msg += f", S3: {s3_key}"
            print(msg)
        except Exception as e:
            print(f"[ERROR] {e}")
            log_failed(BASE_DIR, single.name, str(e))
            traceback.print_exc()
            sys.exit(1)
        return

    INPUT_DIR.mkdir(exist_ok=True)
    pdf_paths = sorted(INPUT_DIR.glob("*.pdf"))
    if not pdf_paths:
        print(f"[INFO] No PDFs in '{INPUT_DIR}'.")
        print("  Put PDFs in input/ or run: python pipeline_pdf_to_db.py path/to/file.pdf")
        return

    limit = get_process_limit()
    total = len(pdf_paths)
    if limit > 0:
        pdf_paths = pdf_paths[:limit]
        print(f"[INFO] Pipeline: PDF -> extract -> DB{s3_note}")
        print(f"[INFO] PDFs in input: {total}. PROCESS_LIMIT={limit} -> processing first {len(pdf_paths)} only (for testing).")
    else:
        print(f"[INFO] Pipeline: PDF -> extract -> DB{s3_note}")
        print(f"[INFO] PDFs in input: {len(pdf_paths)}")
    print("-" * 60)
    ok = 0
    for i, pdf_path in enumerate(pdf_paths, 1):
        print(f"[{i}/{len(pdf_paths)}] {pdf_path.name}")
        try:
            _, s3_key, bill_process_id = run_one_pdf(pdf_path)
            data = json.loads((EXTRACTED_DIR / f"{pdf_path.stem}.json").read_text(encoding="utf-8"))
            line_count = len(data.get("line_items") or [])
            if s3_key:
                s3_url = get_s3_object_url(s3_key)
                if s3_url:
                    print(f"  -> S3 URL: {s3_url}")
            msg = f"  [OK] Bill_Process_ID: {bill_process_id}, T_Bill_Process: 1, T_Bill_HSN_SAC: {line_count}"
            if s3_key:
                msg += f", S3: {s3_key}"
            print(msg)
            ok += 1
        except Exception as e:
            print(f"  [ERROR] {e}")
            log_failed(BASE_DIR, pdf_path.name, str(e))
            traceback.print_exc()

    print("-" * 60)
    print(f"[INFO] Finished. {ok}/{len(pdf_paths)} PDF(s) processed.")


if __name__ == "__main__":
    main()
