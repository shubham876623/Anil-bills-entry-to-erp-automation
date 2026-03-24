"""
Google Drive API client using a service account.
List PDFs, download, and move to "Processed" folder after success.
"""
import os
from pathlib import Path

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

SCOPES = ["https://www.googleapis.com/auth/drive.readonly", "https://www.googleapis.com/auth/drive.file"]


def get_drive_service(service_account_json: str | None = None):
    """Build Drive API v3 service using service account JSON.
    If service_account_json is provided, use it directly; otherwise fall back to env var."""
    path = (service_account_json or os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON", "")).strip()
    if not path:
        raise ValueError("Set GOOGLE_SERVICE_ACCOUNT_JSON in .env to the path of your service account JSON key.")
    path = Path(path)
    if not path.is_file():
        raise FileNotFoundError(f"Service account JSON not found: {path}")
    creds = service_account.Credentials.from_service_account_file(str(path), scopes=SCOPES)
    return build("drive", "v3", credentials=creds)


def _list_params(use_shared_drive: bool = True, **kwargs) -> dict:
    """Build params dict with Shared Drive flags.
    Always include shared drive flags — they work for both personal and shared drives."""
    kwargs["supportsAllDrives"] = True
    kwargs["includeItemsFromAllDrives"] = True
    return kwargs


def _list_pdf_page(service, folder_id: str, page_token) -> tuple[list, str | None]:
    """List one page of PDFs in folder_id."""
    params = _list_params(
        q=f"'{folder_id}' in parents and mimeType='application/pdf' and trashed=false",
        pageSize=100,
        fields="nextPageToken, files(id, name)",
        orderBy="createdTime",
    )
    if page_token:
        params["pageToken"] = page_token
    r = service.files().list(**params).execute()
    files = [{"id": f["id"], "name": f.get("name") or "unknown.pdf", "parent_id": folder_id} for f in r.get("files", [])]
    return files, r.get("nextPageToken")


def _list_subfolders(service, folder_id: str, exclude_name: str = "Processed") -> list[dict]:
    """List direct subfolders, excluding one by name."""
    params = _list_params(
        q=f"'{folder_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false",
        pageSize=100,
        fields="files(id, name)",
    )
    r = service.files().list(**params).execute()
    return [f for f in r.get("files", []) if (f.get("name") or "").strip() != exclude_name]


def get_folder_id_by_path(service, root_id: str, path: str) -> str | None:
    """Resolve a folder by path (e.g. '2026/03 MAR') under root_id."""
    path = (path or "").strip().strip("/")
    if not path:
        return root_id
    current_id = root_id
    for name in (p.strip() for p in path.split("/") if p.strip()):
        subs = _list_subfolders(service, current_id, exclude_name="")
        found = next((s["id"] for s in subs if (s.get("name") or "").strip() == name), None)
        if not found:
            return None
        current_id = found
    return current_id


def find_folder_by_name(service, root_id: str, folder_name: str) -> str | None:
    """Recursively search for a folder by name (case-insensitive) under root_id."""
    folder_name = (folder_name or "").strip()
    if not folder_name:
        return root_id
    target = folder_name.lower()

    def search(fid: str) -> str | None:
        for s in _list_subfolders(service, fid, exclude_name="Processed"):
            if (s.get("name") or "").strip().lower() == target:
                return s["id"]
            found = search(s["id"])
            if found:
                return found
        return None

    return search(root_id)


def list_pdf_files_in_folder(service, folder_id: str, recursive: bool = True) -> list[dict]:
    """List all PDFs in folder (and subfolders except 'Processed'). Returns [{id, name, parent_id}]."""
    all_pdfs = []

    def collect(fid: str):
        token = None
        while True:
            page, token = _list_pdf_page(service, fid, token)
            all_pdfs.extend(page)
            if not token:
                break
        if recursive:
            for sub in _list_subfolders(service, fid):
                collect(sub["id"])

    collect(folder_id)
    return all_pdfs


def download_file(service, file_id: str, dest_path: Path) -> None:
    """Download a Drive file by ID to dest_path."""
    request = service.files().get_media(fileId=file_id)
    with open(dest_path, "wb") as f:
        downloader = MediaIoBaseDownload(f, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()


def get_or_create_processed_folder(service, parent_id: str, name: str = "Processed") -> str:
    """Get or create a 'Processed' subfolder. Returns folder ID."""
    q = f"'{parent_id}' in parents and mimeType='application/vnd.google-apps.folder' and name='{name}' and trashed=false"
    files = service.files().list(
        q=q, fields="files(id)", pageSize=1,
        supportsAllDrives=True, includeItemsFromAllDrives=True,
    ).execute().get("files", [])
    if files:
        return files[0]["id"]
    body = {"name": name, "mimeType": "application/vnd.google-apps.folder", "parents": [parent_id]}
    return service.files().create(body=body, fields="id", supportsAllDrives=True).execute()["id"]


def move_file_to_folder(service, file_id: str, new_folder_id: str, previous_parent_id: str) -> None:
    """Move a file between folders in Drive."""
    service.files().update(
        fileId=file_id, addParents=new_folder_id, removeParents=previous_parent_id,
        supportsAllDrives=True,
    ).execute()
