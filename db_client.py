"""
MSSQL Server connection and insert for extracted invoice data.
Uses .env: ERP_DATABASE_TYPE, ERP_HOST_ADDRESS, ERP_PORT, ERP_DATABASE_NAME, ERP_USERNAME, ERP_PASSWORD.
"""
import os
import re
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Optional, Union

import pyodbc


# ---------------------------------------------------------------------------
# Connection management
# ---------------------------------------------------------------------------

def _detect_sql_server_driver() -> str:
    """Return the best available SQL Server ODBC driver name, or raise if none found."""
    env_driver = (os.environ.get("ERP_ODBC_DRIVER") or "").strip()
    if env_driver:
        return env_driver if env_driver.startswith("{") else f"{{{env_driver}}}"

    available = pyodbc.drivers()
    preferred = [
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
        "ODBC Driver 13 for SQL Server",
        "ODBC Driver 11 for SQL Server",
        "SQL Server Native Client 11.0",
        "SQL Server",
    ]
    for name in preferred:
        if name in available:
            return f"{{{name}}}"
    for name in available:
        if "sql server" in name.lower():
            return f"{{{name}}}"

    raise RuntimeError(
        f"No SQL Server ODBC driver found. Installed drivers: {available}\n"
        "Install 'ODBC Driver 17 for SQL Server' from https://aka.ms/odbc17 "
        "or set ERP_ODBC_DRIVER in .env."
    )


def get_connection():
    """Create and return a connection to MSSQL Server."""
    db_type = (os.environ.get("ERP_DATABASE_TYPE") or "").strip()
    if db_type.upper() != "SQL SERVER":
        raise ValueError(
            f"ERP_DATABASE_TYPE must be 'SQL Server' (current: {db_type!r}). "
            "Set ERP_DATABASE_TYPE=SQL Server in .env"
        )
    host = os.environ.get("ERP_HOST_ADDRESS", "localhost")
    port = os.environ.get("ERP_PORT", "1433")
    database = os.environ.get("ERP_DATABASE_NAME", "")
    user = os.environ.get("ERP_USERNAME", "")
    password = os.environ.get("ERP_PASSWORD", "")
    if not database or not user:
        raise ValueError("Set ERP_DATABASE_NAME and ERP_USERNAME (and ERP_PASSWORD) in .env")
    timeout = int(os.environ.get("ERP_CONNECTION_TIMEOUT", "30"))
    driver = _detect_sql_server_driver()
    conn_str = (
        f"Driver={driver};Server={host},{port};Database={database};"
        f"UID={user};PWD={password};ConnectionTimeout={timeout};"
        "Encrypt=yes;TrustServerCertificate=yes"
    )
    return pyodbc.connect(conn_str)


@contextmanager
def _db_connection(conn=None):
    """Context manager: reuse an existing connection or open (and auto-close) a new one."""
    own = conn is None
    if own:
        conn = get_connection()
    try:
        yield conn
    finally:
        if own:
            conn.close()


# ---------------------------------------------------------------------------
# Lookup helpers (all accept an optional conn to avoid extra round-trips)
# ---------------------------------------------------------------------------

def get_supplier_id_by_gst(gst_no: str, conn=None) -> Optional[int]:
    """Get Supplier_ID from T_Supplier by GST_No. Returns None if not found."""
    if not gst_no or not str(gst_no).strip():
        return None
    gst_clean = str(gst_no).strip()
    with _db_connection(conn) as c:
        row = c.cursor().execute(
            "SELECT Supplier_ID FROM T_Supplier WHERE LTRIM(RTRIM(GST_No)) = ?",
            (gst_clean,),
        ).fetchone()
        return int(row[0]) if row and row[0] is not None else None


def get_supplier_type_of_bill(supplier_id: int, conn=None) -> Optional[str]:
    """Get Type_Of_Bill ('Goods' or 'Service') for a Supplier_ID."""
    if supplier_id is None:
        return None
    with _db_connection(conn) as c:
        row = c.cursor().execute(
            "SELECT Type_Of_Bill FROM T_Supplier WHERE Supplier_ID = ?",
            (int(supplier_id),),
        ).fetchone()
        if row is None or row[0] is None:
            return None
        val = str(row[0]).strip()
        return "Service" if val.upper() == "SERVICE" else "Goods"


def _get_gst_rate(code: str, supplier_id: Optional[int], is_service: bool, conn=None) -> Optional[float]:
    """Lookup GST rate from T_Supplier_HSN (Goods) or T_Supplier_SAC (Service)."""
    if not code or not str(code).strip():
        return None
    code = str(code).strip()
    table = "T_Supplier_SAC" if is_service else "T_Supplier_HSN"
    col = "SAC_NO" if is_service else "HSN_NO"
    with _db_connection(conn) as c:
        cursor = c.cursor()
        if supplier_id is not None:
            cursor.execute(
                f"SELECT GST_RATE FROM {table} WHERE Supplier_ID = ? AND LTRIM(RTRIM({col})) = ?",
                (supplier_id, code),
            )
        else:
            cursor.execute(
                f"SELECT GST_RATE FROM {table} WHERE LTRIM(RTRIM({col})) = ?",
                (code,),
            )
        row = cursor.fetchone()
        return float(row[0]) if row and row[0] is not None else None


def get_uom_id(unit_name: Optional[str], conn=None) -> int:
    """Get UOM_ID from T_UOM by name. Returns 1 (default) if not found."""
    if not unit_name or not str(unit_name).strip():
        return 1
    name = str(unit_name).strip()
    _NORMALIZE = {
        "pc": "Pcs", "pcs": "Pcs", "nos": "Pcs", "no": "Pcs",
        "unit": "unit", "ltr": "Ltr", "litre": "Ltr",
        "kg": "Kg", "mtr": "Mtr", "ft": "FT",
    }
    name = _NORMALIZE.get(name.lower(), name)
    with _db_connection(conn) as c:
        cursor = c.cursor()
        row = cursor.execute(
            "SELECT UOM_ID FROM T_UOM WHERE LOWER(LTRIM(RTRIM(UOM_Name))) = LOWER(?)",
            (name,),
        ).fetchone()
        return int(row[0]) if row and row[0] is not None else 1


def verify_connection() -> bool:
    """Test database connectivity. Returns True if OK, raises on failure."""
    with _db_connection() as conn:
        conn.cursor().execute("SELECT 1").fetchone()
        return True


# ---------------------------------------------------------------------------
# Date parsing
# ---------------------------------------------------------------------------

_MONTH_MAP = {
    "jan": "01", "feb": "02", "mar": "03", "apr": "04", "may": "05", "jun": "06",
    "jul": "07", "aug": "08", "sep": "09", "oct": "10", "nov": "11", "dec": "12",
    "january": "01", "february": "02", "march": "03", "april": "04",
    "june": "06", "july": "07", "august": "08", "september": "09",
    "october": "10", "november": "11", "december": "12",
}


def _normalize_date_string(s: str) -> str:
    """Convert month names to numeric (locale-independent) so strptime never fails on 'Feb' etc."""
    parts = re.split(r"[\s./-]+", s.strip())
    normalized = []
    for p in parts:
        mapped = _MONTH_MAP.get(p.lower())
        if mapped:
            normalized.append(mapped)
        else:
            normalized.append(p)
    return "-".join(normalized)


def _parse_bill_date(date_str: Optional[str]):
    """Parse invoice date string to date object. 2-digit years become 20xx.
    Raises ValueError if date is missing or unparseable (never silently uses today's date)."""
    if not date_str or not str(date_str).strip():
        raise ValueError("invoice_date is missing or empty — cannot use today's date as Bill_Date")

    s = str(date_str).strip()
    # Normalize month names to numbers so parsing is locale-independent
    normalized = _normalize_date_string(s)

    for fmt in (
        "%d-%m-%Y", "%d-%m-%y",
        "%Y-%m-%d",
    ):
        try:
            d = datetime.strptime(normalized, fmt).date()
            if d.year < 100:
                d = d.replace(year=2000 + (d.year % 100))
            return d
        except ValueError:
            continue

    # Also try original string with locale-dependent formats as last resort
    for fmt in (
        "%d.%m.%Y", "%d.%m.%y",
        "%d %b %Y", "%d %B %Y",
        "%d-%b-%Y", "%d-%B-%Y",
        "%d/%m/%Y", "%d/%m/%y",
    ):
        try:
            d = datetime.strptime(s, fmt).date()
            if d.year < 100:
                d = d.replace(year=2000 + (d.year % 100))
            return d
        except ValueError:
            continue

    raise ValueError(f"Could not parse invoice_date: '{date_str}' — cannot use today's date as Bill_Date")


# ---------------------------------------------------------------------------
# Location / Inv_ID mapping
# ---------------------------------------------------------------------------

_INV_CODE_TO_ID = {
    "FSEZ": 8, "FALTA": 8,
    "KAL": 9, "K": 9, "KALYANI": 9,
    "KIL": 2,
    "MGM": 12, "MADHYAGRAM": 12,
    "RAMG": 14, "RAMNAGAR": 14,
}

_INV_ID_TO_CODE = {8: "FSEZ", 9: "KAL", 2: "KIL", 12: "MGM", 14: "RAMG"}


def _resolve_location(pertain_to_raw: str) -> tuple[int, str]:
    """Return (Inv_ID, Pertain_To code) from the extracted pertain_to string."""
    raw = (pertain_to_raw or "").strip().upper()
    compact = raw.replace(" ", "")[:20]

    inv_id = _INV_CODE_TO_ID.get(compact) or _INV_CODE_TO_ID.get(raw[:10].replace(" ", ""))
    if inv_id is None and raw:
        if "FALTA" in raw or "FSEZ" in raw:
            inv_id = 8
        elif "KALYANI" in raw or (raw.startswith("K") and len(raw) <= 3):
            inv_id = 9
        elif "MADHYAGRAM" in raw or "MGM" in raw:
            inv_id = 12
        elif "RAMNAGAR" in raw or "RAMG" in raw:
            inv_id = 14
        elif "KIL" in raw or "SALT LAKE" in raw or "KARIWALA" in raw or "HO" in raw:
            inv_id = 2
    if inv_id is None:
        inv_id = 2  # default: KIL

    return inv_id, _INV_ID_TO_CODE.get(inv_id, "KIL")


# ---------------------------------------------------------------------------
# Tax computation
# ---------------------------------------------------------------------------

def _compute_tax_from_line_items(
    line_items: list,
    gst_number: str,
    type_of_bill: Optional[str],
    supplier_id: Optional[int],
    conn=None,
) -> tuple[float, float, float, float]:
    """
    Compute (taxable, cgst, sgst, igst) from line items.
    GSTIN starting with '19' = within West Bengal → CGST+SGST; otherwise → IGST.
    Reuses a single DB connection for all rate lookups.
    """
    within_state = (gst_number or "").strip().upper().startswith("19")
    is_service = (type_of_bill or "").strip().upper() == "SERVICE"

    taxable_total = cgst_total = sgst_total = igst_total = 0.0

    with _db_connection(conn) as c:
        for item in line_items or []:
            try:
                amt = float(item.get("amount") or 0)
            except (TypeError, ValueError):
                continue
            if amt <= 0:
                continue

            # Lookup GST rate from DB, fallback to extracted tax_rate
            code = item.get("sac_code") if is_service else item.get("hsn_code")
            rate = _get_gst_rate(code, supplier_id, is_service, conn=c) if code else None
            if rate is None:
                try:
                    rate = float(item.get("tax_rate") or 0)
                except (TypeError, ValueError):
                    rate = 0.0

            if rate and rate > 0:
                # Total rates: 5, 12, 18, 28. Per-leg rates: 2.5, 6, 9, 14.
                if within_state and rate not in (5, 12, 18, 28) and rate <= 14:
                    total_tax = amt * (2.0 * rate / 100.0)
                else:
                    total_tax = amt * (rate / 100.0)
            else:
                total_tax = 0.0

            taxable_total += amt
            if within_state:
                cgst_total += total_tax / 2.0
                sgst_total += total_tax / 2.0
            else:
                igst_total += total_tax

    return (
        round(taxable_total, 2),
        round(cgst_total, 2),
        round(sgst_total, 2),
        round(igst_total, 2),
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _created_by_user_id(created_by: Union[int, str, None]) -> int:
    """Resolve Created_By to an integer user ID."""
    if isinstance(created_by, int):
        return created_by
    if isinstance(created_by, str) and created_by.strip().isdigit():
        return int(created_by.strip())
    try:
        return int(os.environ.get("ERP_USER_ID", "0"))
    except (TypeError, ValueError):
        return 0


def _safe_float(value, default=0.0) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _is_identity_column(cursor, table: str, column: str) -> bool:
    """Check if a column is an IDENTITY column."""
    cursor.execute(
        "SELECT COLUMNPROPERTY(OBJECT_ID(?), ?, 'IsIdentity')",
        (table, column),
    )
    row = cursor.fetchone()
    return bool(row and row[0] == 1)


def _get_next_sr_no(cursor) -> int:
    """Get next Sr_No for Normal bills only (CC_Bill_GST=0).
    CC bills and Normal bills have separate Sr_No sequences in T_Bill_Process."""
    cursor.execute(
        "SELECT TOP 1 Sr_No FROM T_Bill_Process WHERE ISNULL(CC_Bill_GST, 0) = 0 ORDER BY Sr_No DESC"
    )
    row = cursor.fetchone()
    if row is None or row[0] is None:
        return 1
    val = row[0]
    if isinstance(val, int):
        return val + 1
    s = str(val).strip()
    if s.isdigit():
        return int(s) + 1
    m = re.search(r"(\d+)\s*$", s)
    return (int(m.group(1)) + 1) if m else 1


# ---------------------------------------------------------------------------
# Payload builder
# ---------------------------------------------------------------------------

def _build_bill_process_payload(
    extracted: dict[str, Any],
    supplier_id: int,
    type_of_bill: str,
    created_by: Union[int, str, None],
    sr_no: int,
    conn=None,
) -> dict[str, Any]:
    """Build T_Bill_Process column values from extracted data. Single-connection."""
    gst_number = (extracted.get("gst_number") or "").strip()
    bill_no = (extracted.get("invoice_number") or "").strip() or None
    bill_date = _parse_bill_date(extracted.get("invoice_date"))
    total_amount = _safe_float(extracted.get("total_amount"))

    # Compute tax from line items (reuses conn for all rate lookups)
    taxable, cgst, sgst, igst = _compute_tax_from_line_items(
        extracted.get("line_items") or [], gst_number, type_of_bill, supplier_id, conn=conn,
    )

    # Prefer extracted tax amounts from the bill's tax summary when available
    ex_cgst = _safe_float(extracted.get("cgst_amount"), default=None)
    ex_sgst = _safe_float(extracted.get("sgst_amount"), default=None)
    ex_igst = _safe_float(extracted.get("igst_amount"), default=None)

    if ex_cgst is not None or ex_sgst is not None or ex_igst is not None:
        cgst = ex_cgst if ex_cgst is not None else 0.0
        sgst = ex_sgst if ex_sgst is not None else 0.0
        igst = ex_igst if ex_igst is not None else 0.0
    elif (cgst + sgst + igst) <= 0 and total_amount > 0 and taxable > 0 and total_amount > taxable:
        # Infer tax from total - taxable difference
        inferred = round(total_amount - taxable, 2)
        if gst_number.upper().startswith("19"):
            cgst = round(inferred / 2.0, 2)
            sgst = round(inferred - cgst, 2)
        else:
            igst = round(inferred, 2)

    inv_id, pertain_to = _resolve_location(extracted.get("pertain_to") or "")
    user_id = _created_by_user_id(created_by)

    return {
        "Sr_No": sr_no,
        "Pertain_To": pertain_to,
        "Inv_ID": inv_id,
        "Prepaid_Bill": False,
        "CC_Bill_GST": False,
        "Bill_Status_ID": 1,
        "Bill_No": bill_no,
        "Received_Date": datetime.now().date(),
        "Bill_Date": bill_date,
        "Supplier_ID": supplier_id,
        "Inv_Amount_B_GST": taxable,
        "CGST_Amount": float(cgst) if cgst else 0.0,
        "SGST_Amount": float(sgst) if sgst else 0.0,
        "IGST_Amount": float(igst) if igst else 0.0,
        "Bill_Amount": total_amount,
        "Edit_Flag": 0,
        "Delete_Flag": 0,
        "Confirm_Stat": "Draft",
        "Created_By": user_id,
        "Modified_By": user_id,
        "Parent_ID": 1,
    }


# ---------------------------------------------------------------------------
# HSN/SAC line-item insert
# ---------------------------------------------------------------------------

def _insert_bill_hsn_sac(
    line_items: list[dict[str, Any]],
    bill_process_id: int,
    user_id: int,
    cursor,
    conn,
) -> int:
    """Insert line items into T_Bill_HSN_SAC. Returns row count."""
    if not line_items:
        return 0

    is_identity = _is_identity_column(cursor, "T_Bill_HSN_SAC", "Bill_HSN_SAC_ID")

    next_id = None
    if not is_identity:
        cursor.execute("SELECT ISNULL(MAX(Bill_HSN_SAC_ID), 0) FROM T_Bill_HSN_SAC")
        row = cursor.fetchone()
        next_id = (int(row[0]) + 1) if row and row[0] is not None else 1

    count = 0
    for item in line_items:
        hsn_code = (item.get("hsn_code") or "").strip()
        sac_code = (item.get("sac_code") or "").strip()
        hsn_sac_type = "SAC" if sac_code else "HSN"
        hsn_sac_no = (hsn_code or sac_code or "").strip() or None
        if hsn_sac_no:
            if hsn_sac_type == "HSN" and len(hsn_sac_no) > 4:
                hsn_sac_no = hsn_sac_no[:4]
            hsn_sac_no = hsn_sac_no[:50]

        qty = _safe_float(item.get("quantity"))
        rate = _safe_float(item.get("rate_per_unit"))
        amount = _safe_float(item.get("amount"))
        uom_id = get_uom_id(item.get("unit"), conn=conn)

        row_values = (bill_process_id, hsn_sac_type, hsn_sac_no, qty, uom_id, rate, amount, 0, 0, user_id, 1)

        if is_identity:
            cursor.execute(
                """INSERT INTO T_Bill_HSN_SAC
                   (Bill_Process_ID, HSN_SAC, HSN_SAC_No, Quantity, UOM_ID, Rate, Amount,
                    Delete_Flag, Edit_Flag, DOC, Created_By, Parent_ID)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE(), ?, ?)""",
                row_values,
            )
        else:
            cursor.execute(
                """INSERT INTO T_Bill_HSN_SAC
                   (Bill_HSN_SAC_ID, Bill_Process_ID, HSN_SAC, HSN_SAC_No, Quantity, UOM_ID, Rate, Amount,
                    Delete_Flag, Edit_Flag, DOC, Created_By, Parent_ID)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE(), ?, ?)""",
                (next_id,) + row_values,
            )
            next_id += 1
        count += 1

    return count


# ---------------------------------------------------------------------------
# Main public API
# ---------------------------------------------------------------------------

def insert_bill_process(extracted: dict[str, Any], created_by: Union[int, str, None] = 0) -> tuple[bool, int]:
    """
    Full insert: validate → T_Bill_Process → T_Bill_HSN_SAC (single transaction, single connection).
    Returns (True, bill_process_id).
    """
    gst_number = (extracted.get("gst_number") or "").strip()
    bill_no = (extracted.get("invoice_number") or "").strip() or None

    conn = get_connection()
    try:
        cursor = conn.cursor()

        # 1. Lookup supplier
        supplier_id = get_supplier_id_by_gst(gst_number, conn=conn)
        if supplier_id is None:
            raise ValueError(f"Supplier not found for GST_No: {gst_number}")

        # 2. Duplicate check
        if bill_no:
            cursor.execute(
                "SELECT Bill_Process_ID FROM T_Bill_Process WHERE Bill_No = ? AND Supplier_ID = ?",
                (bill_no, supplier_id),
            )
            if cursor.fetchone():
                raise ValueError(
                    f"Duplicate Bill_No: '{bill_no}' already exists for Supplier_ID {supplier_id}"
                )

        # 3. Build payload (tax rates, UOM lookups all reuse this connection)
        type_of_bill = get_supplier_type_of_bill(supplier_id, conn=conn) or "Goods"
        sr_no = _get_next_sr_no(cursor)
        payload = _build_bill_process_payload(
            extracted, supplier_id, type_of_bill, created_by, sr_no, conn=conn,
        )

        # 4. Insert T_Bill_Process
        is_identity = _is_identity_column(cursor, "T_Bill_Process", "Bill_Process_ID")
        user_id = payload["Created_By"]

        param_values = (
            payload["Sr_No"], payload["Pertain_To"], payload["Inv_ID"],
            payload["Prepaid_Bill"], payload["CC_Bill_GST"], payload["Bill_Status_ID"],
            payload["Bill_No"], payload["Received_Date"], payload["Bill_Date"],
            payload["Supplier_ID"], payload["Inv_Amount_B_GST"],
            payload["CGST_Amount"], payload["SGST_Amount"], payload["IGST_Amount"],
            payload["Bill_Amount"], payload["Edit_Flag"], payload["Delete_Flag"],
            payload["Confirm_Stat"], payload["Created_By"], payload["Modified_By"],
            payload["Parent_ID"],
        )

        if is_identity:
            cursor.execute(
                """INSERT INTO T_Bill_Process
                   (Sr_No, Pertain_To, Inv_ID, Prepaid_Bill, CC_Bill_GST, Bill_Status_ID, Bill_No,
                    Received_Date, Bill_Date, Supplier_ID, Inv_Amount_B_GST, CGST_Amount, SGST_Amount, IGST_Amount,
                    Bill_Amount, Edit_Flag, Delete_Flag, Confirm_Stat, DOC, DOM, Created_By, Modified_By, Parent_ID)
                   OUTPUT INSERTED.Bill_Process_ID
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE(), GETDATE(), ?, ?, ?)""",
                param_values,
            )
            row = cursor.fetchone()
            bill_process_id = int(float(row[0])) if row and row[0] is not None else None
        else:
            cursor.execute("SELECT ISNULL(MAX(Bill_Process_ID), 0) + 1 FROM T_Bill_Process")
            row = cursor.fetchone()
            bill_process_id = int(row[0]) if row and row[0] is not None else 1
            cursor.execute(
                """INSERT INTO T_Bill_Process
                   (Bill_Process_ID, Sr_No, Pertain_To, Inv_ID, Prepaid_Bill, CC_Bill_GST, Bill_Status_ID, Bill_No,
                    Received_Date, Bill_Date, Supplier_ID, Inv_Amount_B_GST, CGST_Amount, SGST_Amount, IGST_Amount,
                    Bill_Amount, Edit_Flag, Delete_Flag, Confirm_Stat, DOC, DOM, Created_By, Modified_By, Parent_ID)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE(), GETDATE(), ?, ?, ?)""",
                (bill_process_id,) + param_values,
            )

        if bill_process_id is None:
            conn.rollback()
            raise ValueError("Could not get Bill_Process_ID after insert.")

        # 5. Insert line items (same connection, same transaction)
        _insert_bill_hsn_sac(
            extracted.get("line_items") or [],
            bill_process_id, user_id, cursor, conn,
        )

        conn.commit()
        return True, bill_process_id

    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def insert_bill_photo(
    bill_process_id: int,
    photo_name: str,
    s3_key: str,
    created_by: Union[int, str, None] = 0,
    conn=None,
) -> bool:
    """Insert one row into T_Bill_Photo for an S3-uploaded PDF."""
    if not bill_process_id or not s3_key:
        return False
    photo_name = (photo_name or "").strip()[:500] or None
    s3_key = (s3_key or "").strip()[:500] or None
    if not s3_key:
        return False

    user_id = _created_by_user_id(created_by)
    own_conn = conn is None
    if own_conn:
        conn = get_connection()
    try:
        cursor = conn.cursor()
        is_identity = _is_identity_column(cursor, "T_Bill_Photo", "Bill_Photo_ID")

        if is_identity:
            cursor.execute(
                """INSERT INTO T_Bill_Photo
                   (Bill_Process_ID, Photo_Name, Image_Path_Normal, Image_Path_S,
                    Delete_Flag, Edit_Flag, DOC, DOM, Created_By, Modified_By, Parent_ID)
                   VALUES (?, ?, ?, ?, 0, 0, GETDATE(), GETDATE(), ?, ?, 1)""",
                (bill_process_id, photo_name, s3_key, s3_key, user_id, user_id),
            )
        else:
            cursor.execute("SELECT ISNULL(MAX(Bill_Photo_ID), 0) + 1 FROM T_Bill_Photo")
            row = cursor.fetchone()
            next_id = int(row[0]) if row and row[0] is not None else 1
            cursor.execute(
                """INSERT INTO T_Bill_Photo
                   (Bill_Photo_ID, Bill_Process_ID, Photo_Name, Image_Path_Normal, Image_Path_S,
                    Delete_Flag, Edit_Flag, DOC, DOM, Created_By, Modified_By, Parent_ID)
                   VALUES (?, ?, ?, ?, ?, 0, 0, GETDATE(), GETDATE(), ?, ?, 1)""",
                (next_id, bill_process_id, photo_name, s3_key, s3_key, user_id, user_id),
            )
        if own_conn:
            conn.commit()
        return True
    except pyodbc.Error:
        if own_conn:
            conn.rollback()
        raise
    finally:
        if own_conn:
            conn.close()
