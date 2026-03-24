"""Quick test: list PDFs from GOOGLE_DRIVE_FOLDER_ID and print count + names."""
import os
import sys
from pathlib import Path

# Load .env
env_path = Path(__file__).resolve().parent / ".env"
if env_path.is_file():
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, _, v = line.partition("=")
            k, v = k.strip(), v.strip().strip('"').strip("'")
            if k:
                os.environ.setdefault(k, v)

folder_id = os.environ.get("GOOGLE_DRIVE_FOLDER_ID", "").strip()
if not folder_id:
    print("GOOGLE_DRIVE_FOLDER_ID not set in .env")
    sys.exit(1)
subfolder = os.environ.get("GOOGLE_DRIVE_SUBFOLDER", "").strip().strip('"').strip("'")

from google_drive_client import get_drive_service, get_folder_id_by_path, list_pdf_files_in_folder

print(f"Folder ID: {folder_id}")
if subfolder:
    print(f"Subfolder path: {subfolder!r}")
print("Connecting...")
service = get_drive_service()
target_id = folder_id
if subfolder:
    target_id = get_folder_id_by_path(service, folder_id, subfolder)
    if not target_id:
        print(f"ERROR: Subfolder not found: {subfolder!r}")
        sys.exit(1)
    print(f"Resolved target folder ID: {target_id}")
print("Listing PDFs (this folder only, no recursion)...")
try:
    files = list_pdf_files_in_folder(service, target_id, recursive=False)
except Exception as e:
    print(f"ERROR: {e}")
    if "404" in str(e):
        print("Tip: Share the folder with your service account and set GOOGLE_DRIVE_FOLDER_ID to that folder's ID.")
    sys.exit(1)
pdfs = [f for f in files if (f.get("name") or "").lower().endswith(".pdf")]
print(f"Total returned: {len(pdfs)}")
for i, f in enumerate(pdfs, 1):
    print(f"  {i}. {f.get('name', '?')}")
