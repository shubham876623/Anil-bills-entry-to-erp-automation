"""
Invoice extractor: upload PDF to Anthropic Files API, then extract structured data via Claude.
Set ANTHROPIC_API_KEY in .env or in your environment.
"""
import json
import os
import re
from pathlib import Path

from dotenv import load_dotenv

import anthropic

# Load .env from the same folder as this script so ANTHROPIC_API_KEY is set
load_dotenv(Path(__file__).resolve().parent / ".env")

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "").strip()

# Extraction prompt: include GRN number (e.g. GRN/K/26425) and full line items
EXTRACTION_PROMPT = """
This document is an Indian GST bill/invoice or GRN. Extract ALL pages and EVERY line item.
Also extract the GRN number if present (e.g. "GRN/K/26425" in a box or header).
Return a single JSON object (no markdown) with this structure:
{
  "grn_number": "string or null",
  "invoice_number": "string or null",
  "invoice_date": "string or null",
  "total_amount": number or null,
  "round_off": number or null,
  "gst_number": "string or null",
  "supplier_name": "string or null (seller/vendor who issued the invoice—NOT Billed to)",
  "buyer_name": "string or null (Billed to / Shipped to = customer)",
  "bill_to_address": "string or null",
  "pertain_to": "string or null",
  "cgst_amount": number or null,
  "sgst_amount": number or null,
  "igst_amount": number or null,
  "line_items": [
    {
      "description": "string",
      "hsn_code": "string",
      "quantity": number,
      "unit": "string",
      "rate_per_unit": number,
      "amount": number,
      "tax_rate": number or null
    }
  ]
}

Rules:
0) Supplier vs Buyer (critical): On the invoice, the SELLER appears at top/letterhead (e.g. PETEX) and the CUSTOMER appears as "Billed to" / "Customer" / "Recipient" (e.g. KARIWALA INDUSTRIES LIMITED). supplier_name = SELLER only (e.g. PETEX). buyer_name = CUSTOMER only (e.g. KARIWALA INDUSTRIES LIMITED). NEVER use the customer name as supplier_name. gst_number = the GSTIN printed next to the SELLER/letterhead (e.g. under PETEX), NOT the "Customer" or "Billing" or "Recipient" GSTIN. Wrong: using 19AABCK1380K1ZE (Kariwala) when the vendor is PETEX. Right: 19AAMFP8972E1ZL when PETEX is the vendor.
1) Invoice number: Copy EXACTLY as printed on the bill. Use SLASH (/) not hyphen (-) where the bill shows a slash (e.g. DCPL/1940/25-26 not DCPL-1940/25-26; RE/20/25-26 not RE/2025-26). Pay close attention when the invoice number is in smaller font—read each segment carefully (e.g. "20" and "25-26" separately so you get RE/20/25-26).
2) invoice_date: Use ONLY the main invoice date at the top of the bill (e.g. "DATE: 12.02.2026" or "Invoice Date: 12-Feb-2026")—NOT the dates in the line-item table or any other date on the page. Always use a 4-digit year (e.g. 2026, not 26). Do NOT swap day and month; do NOT misread the year (2026 not 2006). Return in a clear format: DD.MM.YYYY or DD/MM/YYYY or DD-MMM-YYYY (e.g. 12.02.2026 or 12-Feb-2026).
3) Total amount: Use the final "Amount Payable" or "Total" that appears at the bottom of the bill AFTER round-off. Do not use the sum before round-off. If the bill shows a "Round Off" row, include that and set total_amount to the final payable amount.
4) round_off: Extract the round-off amount if shown (can be positive or negative). If not shown, set null.
5) HSN code: HSN on the invoice can be 4, 6, or 8 digits. Extract exactly as printed—do not add leading zeros. Never pad a 4-digit HSN to 8 digits with zeros.
6) bill_to_address: Extract the full "Bill to" / "Billing address" / "Shipped to" block exactly as printed. Do not use the supplier address.
7) pertain_to (Bill To location code): Look at the "Bill to" / "Billed to" / "Shipped to" / "Shipping address" section. Identify which of these locations the address refers to and return ONLY the corresponding code: Falta Special Economical Zone → FSEZ; Kalyani → KAL; Kariwala Industries Ltd HO (or Salt Lake, Sector V, HO) → KIL; Madhyagram → MGM; Ramnagar → RAMG. If the address contains "Kalyani" or GRN shows "K" (e.g. GRN/K/31805), return KAL. If "Falta" or FSEZ, return FSEZ. If "Madhyagram", return MGM. If "Ramnagar", return RAMG. If "Salt Lake" or "Kariwala" HO / head office, return KIL. Return exactly one of: FSEZ, KAL, KIL, MGM, RAMG (or null if none clearly match).
8) Tax summary: When the bill has a table with "Product Value", "CGST", "SGST", "IGST", "Round Off", "Grand Total", extract the numeric values: cgst_amount, sgst_amount, igst_amount (or 0 for IGST if only CGST+SGST).
9) GRN: Extract if present (e.g. GRN/K/26425). Process every page. One table row = one line_items entry. Use exact values from the PDF (no multiplication).
"""


def extract_invoice(pdf_path: str | Path) -> dict:
    """
    Extract invoice/GRN data from a single PDF. Returns dict with grn_number,
    invoice_number, total_amount, gst_number, line_items, etc.
    """
    if not ANTHROPIC_API_KEY:
        raise ValueError(
            "ANTHROPIC_API_KEY is not set. Add it to .env or set the environment variable."
        )
    path = Path(pdf_path)
    if not path.is_file():
        raise FileNotFoundError(f"PDF not found: {path}")

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    with open(path, "rb") as f:
        file_response = client.beta.files.upload(
            file=(path.name, f, "application/pdf"),
        )
    file_id = file_response.id

    try:
        response = client.beta.messages.create(
            model="claude-sonnet-4-5",
            max_tokens=8192,
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "document",
                            "source": {"type": "file", "file_id": file_id},
                        },
                        {"type": "text", "text": EXTRACTION_PROMPT},
                    ],
                }
            ],
            betas=["files-api-2025-04-14"],
        )
    finally:
        try:
            client.beta.files.delete(file_id)
        except Exception:
            pass

    text = ""
    for block in response.content:
        if getattr(block, "type", None) == "text":
            text = block.text
            break
    if not text:
        raise ValueError("No text in Claude response")

    raw = text.strip()
    if raw.startswith("```"):
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```\s*$", "", raw)
    return json.loads(raw)


def main():
    import sys
    if not ANTHROPIC_API_KEY:
        raise SystemExit(
            "ANTHROPIC_API_KEY is not set. Add it to a .env file in this folder or set the environment variable.\n"
            "Example .env: ANTHROPIC_API_KEY=sk-ant-your-key-here"
        )
    cwd = Path.cwd()
    if len(sys.argv) < 2:
        pdf_paths = sorted(cwd.glob("*.pdf"))
        if not pdf_paths:
            print("No PDF files in current directory.", file=sys.stderr)
            sys.exit(1)
    else:
        pdf_paths = [Path(p) for p in sys.argv[1:]]

    for i, path in enumerate(pdf_paths, 1):
        if not path.is_file():
            print(f"Skip (not found): {path}", file=sys.stderr)
            continue
        print(f"[{i}/{len(pdf_paths)}] {path.name} ...")
        try:
            data = extract_invoice(path)
            print(json.dumps(data, indent=2, ensure_ascii=False))
        except Exception as e:
            print(f"  ERROR: {e}", file=sys.stderr)


if __name__ == "__main__":
    main()
