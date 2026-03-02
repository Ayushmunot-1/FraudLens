"""
Universal File Parser — Phase 3
=================================
Handles all file types your platform accepts:

  1. CSV          — standard comma-separated files
  2. Excel        — single and multi-sheet .xlsx/.xls files
  3. PDF          — digital PDFs with tables (e.g. invoice PDFs)
  4. SAP Export   — SAP-specific Excel/CSV with German column names

Also includes smart column detection — even if the user's columns
are named differently, we map them to our standard schema automatically.
"""

import pandas as pd
import numpy as np
import re
import logging
from typing import Tuple, Optional
from pathlib import Path

logger = logging.getLogger(__name__)


# ─── SAP Column Name Mappings ─────────────────────────────────────────────────
# SAP exports often have German or coded column names — we map them to standard names

SAP_COLUMN_MAP = {
    # German SAP column names
    "belegnummer": "invoice_id",
    "rechnungsnummer": "invoice_id",
    "buchungsdatum": "invoice_date",
    "belegdatum": "invoice_date",
    "lieferant": "vendor",
    "kreditor": "vendor",
    "lieferantenname": "vendor",
    "betrag": "amount",
    "rechnungsbetrag": "amount",
    "nettobetrag": "amount",
    "menge": "quantity",
    "einzelpreis": "unit_price",
    "genehmigt_von": "approved_by",
    "kostenstelle": "department",
    "abteilung": "department",

    # SAP English/code column names
    "belnr": "invoice_id",
    "bldat": "invoice_date",
    "budat": "invoice_date",
    "lifnr": "vendor_id",
    "name1": "vendor",
    "dmbtr": "amount",
    "wrbtr": "amount",
    "menge_sap": "quantity",
    "netpr": "unit_price",
    "ekgrp": "department",
    "document_number": "invoice_id",
    "posting_date": "invoice_date",
    "vendor_number": "vendor_id",
    "vendor_name": "vendor",
    "amount_lc": "amount",
    "gross_amount": "amount",
    "net_amount": "amount",

    # Common variations
    "inv_number": "invoice_id",
    "inv_date": "invoice_date",
    "inv_amount": "amount",
    "supplier_name": "vendor",
    "total": "amount",
    "total_amount": "amount",
    "approved_by": "approved_by",
    "approver_name": "approved_by",
}


class UniversalFileParser:
    """
    Parses any supported file type into a clean pandas DataFrame
    ready for anomaly detection.

    Usage:
        parser = UniversalFileParser()
        df, file_type, notes = parser.parse("invoice_data.pdf")
    """

    def parse(self, file_path: str) -> Tuple[pd.DataFrame, str, str]:
        """
        Main entry point. Returns (dataframe, file_type, notes).
        Notes contains info about what was detected/converted.
        """
        path = Path(file_path)
        ext = path.suffix.lower()

        logger.info(f"Parsing file: {path.name} (type: {ext})")

        if ext == ".csv":
            return self._parse_csv(file_path)
        elif ext in [".xlsx", ".xls"]:
            return self._parse_excel(file_path)
        elif ext == ".pdf":
            return self._parse_pdf(file_path)
        else:
            raise ValueError(f"Unsupported file type: {ext}. Supported: CSV, Excel, PDF")

    # ─── CSV Parser ───────────────────────────────────────────────────────────

    def _parse_csv(self, file_path: str) -> Tuple[pd.DataFrame, str, str]:
        """
        Parses CSV files. Tries multiple encodings and delimiters automatically.
        """
        encodings = ["utf-8", "latin-1", "cp1252", "iso-8859-1"]
        delimiters = [",", ";", "\t", "|"]

        df = None
        for encoding in encodings:
            for delimiter in delimiters:
                try:
                    df = pd.read_csv(file_path, encoding=encoding, sep=delimiter)
                    if len(df.columns) >= 2:  # Valid parse has multiple columns
                        break
                except Exception:
                    continue
            if df is not None and len(df.columns) >= 2:
                break

        if df is None or df.empty:
            raise ValueError("Could not parse CSV file — check the file format")

        df = self._smart_column_mapping(df)
        notes = f"CSV parsed: {len(df)} rows, {len(df.columns)} columns detected"
        logger.info(notes)
        return df, "CSV", notes

    # ─── Excel Parser ─────────────────────────────────────────────────────────

    def _parse_excel(self, file_path: str) -> Tuple[pd.DataFrame, str, str]:
        """
        Parses Excel files. Handles:
        - Single sheet files
        - Multi-sheet files (combines all relevant sheets)
        - SAP-exported Excel files
        - Files where data starts on row 2+ (headers not on row 1)
        """
        xl = pd.ExcelFile(file_path)
        sheet_names = xl.sheet_names
        logger.info(f"Excel file has {len(sheet_names)} sheet(s): {sheet_names}")

        all_dfs = []

        for sheet in sheet_names:
            # Skip sheets that are clearly not data (common SAP sheet names)
            skip_keywords = ["cover", "readme", "instructions", "info", "summary", "chart", "graph"]
            if any(kw in sheet.lower() for kw in skip_keywords):
                logger.info(f"Skipping sheet: {sheet}")
                continue

            try:
                # Try reading with header on row 0
                df = pd.read_excel(file_path, sheet_name=sheet, header=0)

                # If first row looks like another header (all strings), try row 1
                if self._looks_like_header(df.iloc[0] if len(df) > 0 else pd.Series()):
                    df = pd.read_excel(file_path, sheet_name=sheet, header=1)

                if df.empty or len(df.columns) < 2:
                    continue

                # Add sheet name as context if multiple sheets
                if len(sheet_names) > 1:
                    df["_source_sheet"] = sheet

                df = self._smart_column_mapping(df)
                all_dfs.append(df)
                logger.info(f"Sheet '{sheet}': {len(df)} rows")

            except Exception as e:
                logger.warning(f"Could not parse sheet '{sheet}': {e}")
                continue

        if not all_dfs:
            raise ValueError("No usable data found in Excel file")

        # Combine all sheets
        combined = pd.concat(all_dfs, ignore_index=True) if len(all_dfs) > 1 else all_dfs[0]

        sheet_info = f"{len(all_dfs)} sheet(s) combined" if len(all_dfs) > 1 else "1 sheet"
        notes = f"Excel parsed: {len(combined)} rows from {sheet_info}"
        logger.info(notes)
        return combined, "Excel", notes

    # ─── PDF Parser ───────────────────────────────────────────────────────────

    def _parse_pdf(self, file_path: str) -> Tuple[pd.DataFrame, str, str]:
        """
        Parses PDF files containing invoice tables.
        Uses pdfplumber for digital PDFs (created by software).
        Falls back to text extraction and pattern matching for simpler PDFs.

        Note: Scanned/image PDFs require OCR (Tesseract) which is a future enhancement.
        """
        try:
            import pdfplumber
        except ImportError:
            raise ImportError("pdfplumber not installed. Run: pip install pdfplumber")

        all_tables = []
        all_text_data = []

        with pdfplumber.open(file_path) as pdf:
            logger.info(f"PDF has {len(pdf.pages)} page(s)")

            for page_num, page in enumerate(pdf.pages):
                # Strategy 1: Try to extract tables directly
                tables = page.extract_tables()
                if tables:
                    for table in tables:
                        if table and len(table) > 1:
                            try:
                                # First row is header
                                headers = [str(h).strip() if h else f"col_{i}"
                                          for i, h in enumerate(table[0])]
                                rows = table[1:]
                                df = pd.DataFrame(rows, columns=headers)
                                df = df.dropna(how="all")
                                if len(df) > 0 and len(df.columns) >= 2:
                                    all_tables.append(df)
                            except Exception as e:
                                logger.warning(f"Table extraction failed on page {page_num+1}: {e}")

                # Strategy 2: Extract text and look for invoice patterns
                if not tables:
                    text = page.extract_text()
                    if text:
                        extracted = self._extract_invoice_data_from_text(text)
                        if extracted:
                            all_text_data.extend(extracted)

        if all_tables:
            combined = pd.concat(all_tables, ignore_index=True)
            combined = self._smart_column_mapping(combined)
            notes = f"PDF parsed: {len(combined)} rows extracted from {len(all_tables)} table(s)"
            return combined, "PDF", notes

        elif all_text_data:
            df = pd.DataFrame(all_text_data)
            df = self._smart_column_mapping(df)
            notes = f"PDF parsed via text extraction: {len(df)} invoice records found"
            return df, "PDF", notes

        else:
            raise ValueError(
                "Could not extract invoice data from PDF. "
                "The PDF may be a scanned image (OCR support coming soon) "
                "or may not contain structured invoice tables."
            )

    # ─── Smart Column Mapping ─────────────────────────────────────────────────

    def _smart_column_mapping(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Automatically maps column names to our standard schema.
        Works for SAP exports, custom ERP systems, and any naming convention.

        Steps:
        1. Normalize column names (lowercase, strip spaces)
        2. Check against SAP_COLUMN_MAP
        3. Use fuzzy matching for close matches
        4. Detect numeric columns that might be amounts
        """
        # Step 1: Normalize
        df.columns = [str(c).lower().strip().replace(" ", "_").replace("-", "_")
                      for c in df.columns]

        # Step 2: Apply SAP/standard column map
        rename_map = {}
        for col in df.columns:
            if col in SAP_COLUMN_MAP:
                rename_map[col] = SAP_COLUMN_MAP[col]

        if rename_map:
            logger.info(f"Column mapping applied: {rename_map}")
            df.rename(columns=rename_map, inplace=True)

        # Step 3: Fuzzy match remaining unmapped columns
        standard_cols = ["invoice_id", "vendor", "amount", "quantity", "unit_price",
                        "invoice_date", "approved_by", "department"]
        already_mapped = set(df.columns)

        for col in df.columns:
            if col in standard_cols:
                continue
            # Check if column name contains a keyword
            for standard in standard_cols:
                if standard not in already_mapped:
                    keywords = standard.split("_")
                    if any(kw in col for kw in keywords):
                        rename_map[col] = standard
                        already_mapped.add(standard)
                        break

        if rename_map:
            df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns}, inplace=True)

        # Step 4: Convert numeric columns
        for col in ["amount", "quantity", "unit_price"]:
            if col in df.columns:
                # Remove currency symbols and commas before converting
                if df[col].dtype == object:
                    df[col] = df[col].astype(str).str.replace(r'[£$€,₹\s]', '', regex=True)
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # Step 5: Convert date columns
        if "invoice_date" in df.columns:
            df["invoice_date"] = pd.to_datetime(df["invoice_date"], errors="coerce")

        # Step 6: Drop completely empty rows
        df = df.dropna(how="all").reset_index(drop=True)

        return df

    # ─── Text-based Invoice Extraction ───────────────────────────────────────

    def _extract_invoice_data_from_text(self, text: str) -> list:
        """
        Extracts invoice fields from raw PDF text using pattern matching.
        Used as fallback when no structured tables are found.
        """
        records = []
        lines = text.split("\n")

        # Look for common invoice patterns
        invoice_id = None
        vendor = None
        amount = None
        date = None

        for line in lines:
            line = line.strip()
            if not line:
                continue

            # Invoice number patterns
            inv_match = re.search(r'(?:invoice|inv|bill)[#\s\-:]*([A-Z0-9\-]+)', line, re.IGNORECASE)
            if inv_match:
                invoice_id = inv_match.group(1)

            # Amount patterns (looks for currency amounts)
            amount_match = re.search(r'(?:total|amount|due|payable)[:\s]*[$£€₹]?\s*([\d,]+\.?\d*)', line, re.IGNORECASE)
            if amount_match:
                try:
                    amount = float(amount_match.group(1).replace(",", ""))
                except:
                    pass

            # Date patterns
            date_match = re.search(r'\b(\d{1,2}[\/\-\.]\d{1,2}[\/\-\.]\d{2,4})\b', line)
            if date_match:
                date = date_match.group(1)

            # Vendor patterns (line after "From:" or "Vendor:")
            vendor_match = re.search(r'(?:from|vendor|supplier|company)[:\s]+(.+)', line, re.IGNORECASE)
            if vendor_match:
                vendor = vendor_match.group(1).strip()

        # Only add if we found at least an amount
        if amount:
            records.append({
                "invoice_id": invoice_id or "EXTRACTED",
                "vendor": vendor or "Unknown",
                "amount": amount,
                "invoice_date": date,
            })

        return records

    # ─── Helper ───────────────────────────────────────────────────────────────

    def _looks_like_header(self, row: pd.Series) -> bool:
        """Returns True if a row looks like it might be a second header row."""
        if row.empty:
            return False
        str_vals = [str(v).lower() for v in row.values if pd.notna(v)]
        header_keywords = ["invoice", "vendor", "amount", "date", "quantity", "total", "number"]
        return sum(1 for v in str_vals if any(kw in v for kw in header_keywords)) >= 2


# ─── Sample SAP CSV Generator (for testing) ──────────────────────────────────

def generate_sample_sap_csv(output_path: str):
    """
    Creates a sample SAP-format CSV with German column names for testing.
    """
    data = {
        "Belegnummer": ["SAP-2024-001", "SAP-2024-002", "SAP-2024-003", "SAP-2024-004",
                        "SAP-2024-005", "SAP-2024-001", "SAP-2024-007", "SAP-2024-008"],
        "Lieferant": ["Acme GmbH", "TechCorp AG", "Acme GmbH", "FastParts GmbH",
                      "Acme GmbH", "TechCorp AG", "NeuLieferant GmbH", "FastParts GmbH"],
        "Buchungsdatum": ["2024-01-05", "2024-01-06", "2024-01-07", "2024-01-08",
                          "2024-01-13", "2024-01-12", "2024-01-15", "2024-02-03"],
        "Menge": [10, 5, 2, 100, 4, 5, 1, 50],
        "Einzelpreis": [500.00, 1200.00, 500.00, 45.00, 500.00, 1200.00, 80000.00, 45.00],
        "Betrag": [5000.00, 6000.00, 1000.00, 4500.00, 2000.00, 6000.00, 80000.00, 2250.00],
        "Kostenstelle": ["IT", "Finance", "IT", "Operations", "IT", "Finance", "Unknown", "Operations"],
        "Genehmigt_von": ["Hans Mueller", "Sarah Weber", "Hans Mueller", "Mike Schmidt",
                          "Hans Mueller", "Sarah Weber", "", "Mike Schmidt"],
    }
    pd.DataFrame(data).to_csv(output_path, index=False)
    logger.info(f"Sample SAP CSV created at: {output_path}")