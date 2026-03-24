# Bills entry – full pipeline

## Flow

1. **Pick PDF** from Google Drive (folder set in `.env`).
2. **OCR / Extract** – Claude API reads the PDF and extracts invoice data (bill no, date, supplier, line items, HSN, amounts, etc.).
3. **Push to DB** – Data is written to:
   - **T_Bill_Process** (one row per bill)
   - **T_Bill_HSN_SAC** (one row per line item)
4. **Upload PDF to S3** – The same PDF is uploaded to the configured AWS S3 bucket.
5. **T_Bill_Photo** – One row is inserted with `Bill_Process_ID`, file name, and S3 path.
6. **Move to Processed** – The PDF is moved to a “Processed” subfolder in Drive so it is not picked again.

**Extraction rules (OCR):** Invoice number is taken exactly as printed (slashes preserved; small font read carefully, e.g. RE/20/25-26). Total amount is the final “Amount Payable” after round-off; round-off is extracted when shown. HSN is 8-digit with leading zeros preserved. **Bill to address** is extracted and stored in `Bill_To_Address` (add column to `T_Bill_Process` if missing). **GST amounts** go to `CGST_Amount`, `SGST_Amount`, `IGST_Amount` (we infer from total − taxable when rate lookup fails).

## How to run

**Process all PDFs in Drive (or local folder)** – Run the script once; it picks every PDF in the folder, processes them one by one, then exits:

```bash
python automation_watch_drive.py
```

**Manual pipeline (one or more PDFs from `input/` folder or by path):**

```bash
python pipeline_pdf_to_db.py
# or
python pipeline_pdf_to_db.py "path/to/invoice.pdf"
```

## .env

- **Google Drive:** `GOOGLE_DRIVE_FOLDER_ID`, `GOOGLE_SERVICE_ACCOUNT_JSON`
- **Extraction:** `ANTHROPIC_API_KEY`
- **Database:** `ERP_HOST_ADDRESS`, `ERP_PORT`, `ERP_DATABASE_NAME`, `ERP_USERNAME`, `ERP_PASSWORD`
- **S3 (optional):** `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, `S3_BUCKET_NAME`. Uploads use folder `Bill_Images` and key format `Bill_Images/<timestamp>~<filename>` (e.g. `Bill_Images/050225143052~GRN-31428.HANUMANN.IV NO-0419.pdf`). Set `S3_PUBLIC_READ=0` if the bucket blocks public ACLs (then use a bucket policy to allow GetObject).
- **Testing (optional):** `PROCESS_LIMIT=3` – process only the first 3 PDFs per run, then stop. Use for testing with 2–3 bills; remove or set to `0` to process all.
- **Subfolder (optional):** `GOOGLE_DRIVE_SUBFOLDER` – e.g. `2026/03 MAR` to list only PDFs from that path under the folder. Leave empty to use the folder above as-is.

**To pick all PDFs from a specific folder (e.g. "03 MAR" with 16 PDFs):** Use that folder’s ID as `GOOGLE_DRIVE_FOLDER_ID`. Open the folder in Google Drive, copy the ID from the URL (`.../folders/FOLDER_ID`), and **share that folder** with the service account email (from your JSON key). The script lists only direct children of that folder (no recursion).

If S3 is not set, steps 4 and 5 are skipped; the rest of the flow still runs.

**S3 "Access Denied" when opening PDF:** (1) Use the **full key** from `T_Bill_Photo.Image_Path_Normal` as-is when building the URL—do not prepend `Bill_Images/` again (the key is already `Bill_Images/<timestamp>~<filename>.pdf`). (2) If the bucket has "Block public access" enabled, either allow public ACLs for the bucket and keep uploads with `public-read`, or set `S3_PUBLIC_READ=0` in `.env` and add a bucket policy allowing `s3:GetObject` for `Bill_Images/*`.

## Verifying in the database (by Bill_Process_ID)

After a PDF is processed, the script prints **Bill_Process_ID**. Use it to confirm the row in the DB.

**Single bill (T_Bill_Process):**

```sql
SELECT * FROM T_Bill_Process WHERE Bill_Process_ID = <your_id>;
```

**Line items (T_Bill_HSN_SAC) for that bill:**

```sql
SELECT * FROM T_Bill_HSN_SAC WHERE Bill_Process_ID = <your_id> ORDER BY Bill_HSN_SAC_ID;
```

**Photo record (T_Bill_Photo) for that bill:**

```sql
SELECT * FROM T_Bill_Photo WHERE Bill_Process_ID = <your_id>;
```

**All three in one go:**

```sql
SELECT * FROM T_Bill_Process  WHERE Bill_Process_ID = <your_id>;
SELECT * FROM T_Bill_HSN_SAC  WHERE Bill_Process_ID = <your_id> ORDER BY Bill_HSN_SAC_ID;
SELECT * FROM T_Bill_Photo    WHERE Bill_Process_ID = <your_id>;
```

Replace `<your_id>` with the printed value (e.g. `12345`).

## Failed PDFs

When a PDF fails (e.g. **Supplier not found** for a GST number), the script logs the error and appends one line to **`failed_processing.log`** in the project folder:

- Format: `YYYY-MM-DD HH:MM:SS | filename.pdf | error message`
- You can fix suppliers in the DB (or add new ones) and re-run the script; processed PDFs are moved to Processed (or `input/processed`), so only remaining/failed ones are picked again. For Drive, re-add failed PDFs to the source folder if you want them retried.
