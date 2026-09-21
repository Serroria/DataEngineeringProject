"""
==============================================================================
validation.py
Sistem Monitoring dan Validasi Kualitas Data Otomatis
Berbasis Apache Kafka dan Great Expectations

Fungsi:
1. Membuat Great Expectations Data Context
2. Membuat Data Source dan Data Asset
3. Membuat Expectation Suite
4. Menjalankan validasi terhadap micro-batch
5. Menghasilkan hasil validasi
6. Menentukan baris valid / invalid berdasarkan aturan kualitas data
7. Menyimpan laporan hasil validasi

Great Expectations:
1.23.x
==============================================================================
"""

import json
import os
from datetime import datetime

import pandas as pd
import great_expectations as gx


# ============================================================================
# KONFIGURASI
# ============================================================================

OUTPUT_DIR = "data/output"
REPORT_DIR = os.path.join(OUTPUT_DIR, "quality_reports")

os.makedirs(REPORT_DIR, exist_ok=True)


# Negara yang dianggap valid
VALID_COUNTRIES = {
    "Australia",
    "Austria",
    "Bahrain",
    "Belgium",
    "Brazil",
    "Canada",
    "Channel Islands",
    "Cyprus",
    "Czech Republic",
    "Denmark",
    "EIRE",
    "European Community",
    "Finland",
    "France",
    "Germany",
    "Greece",
    "Hong Kong",
    "Iceland",
    "Israel",
    "Italy",
    "Japan",
    "Lebanon",
    "Lithuania",
    "Malta",
    "Netherlands",
    "Norway",
    "Poland",
    "Portugal",
    "RSA",
    "Saudi Arabia",
    "Singapore",
    "Spain",
    "Sweden",
    "Switzerland",
    "USA",
    "United Arab Emirates",
    "United Kingdom",
    "Unspecified",
}


# Kolom wajib
REQUIRED_COLUMNS = [
    "TransactionNo",
    "Date",
    "ProductNo",
    "Price",
    "Quantity",
]


# ============================================================================
# GREAT EXPECTATIONS
# ============================================================================

def create_gx_context():
    """
    Membuat GX Data Context ephemeral.

    Ephemeral context cocok untuk pipeline streaming lokal karena
    konfigurasi dibuat ketika program dijalankan.
    """

    context = gx.get_context(mode="ephemeral")

    return context


def create_expectation_suite(context):
    """
    Membuat Expectation Suite berdasarkan proposal penelitian.

    Dimensi:
    - Completeness
    - Validity
    - Uniqueness
    - Consistency
    - Accuracy
    """

    suite = gx.ExpectationSuite(
        name="sales_transaction_quality_suite"
    )

    # ------------------------------------------------------------------------
    # COMPLETENESS
    # ------------------------------------------------------------------------

    for column in REQUIRED_COLUMNS:

        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column=column
            )
        )

    # CustomerNo boleh NULL menurut desain penelitian.
    # Jadi tidak dijadikan expectation wajib tidak-null.

    # ------------------------------------------------------------------------
    # VALIDITY - TIPE DATA
    # ------------------------------------------------------------------------

    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeOfType(
            column="Price",
            type_="FLOAT",
        )
    )

    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeOfType(
            column="Quantity",
            type_="INTEGER",
        )
    )

    # ------------------------------------------------------------------------
    # VALIDITY - FORMAT TANGGAL
    # ------------------------------------------------------------------------
    #
    # Date dari dataset awal berbentuk:
    # 12/9/2019
    #
    # Kita melakukan parsing menggunakan pandas di pipeline.
    # Expectation berikut memastikan nilai Date tidak kosong.
    #
    # Pemeriksaan format sebenarnya juga dilakukan di fungsi
    # row_level_validation() di bawah.

    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="Date"
        )
    )

    # ------------------------------------------------------------------------
    # CONSISTENCY
    # ------------------------------------------------------------------------

    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="Country",
            value_set=sorted(VALID_COUNTRIES),
        )
    )

    # ------------------------------------------------------------------------
    # ACCURACY - PRICE
    # ------------------------------------------------------------------------
    #
    # Proposal menyatakan Price harus > 0.
    #
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="Price",
            min_value=0.000001,
            max_value=1000000,
        )
    )

    # ------------------------------------------------------------------------
    # ACCURACY - QUANTITY
    # ------------------------------------------------------------------------
    #
    # Untuk implementasi ini kita menetapkan Quantity > 0.
    #
    # Hal ini mengikuti skenario proposal yang memperlakukan nilai quantity
    # ekstrem/negatif sebagai anomaly untuk demonstrasi.
    #
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="Quantity",
            min_value=1,
            max_value=10000,
        )
    )

    return suite


def run_gx_validation(df):
    """
    Menjalankan Great Expectations terhadap satu micro-batch.

    Parameters
    ----------
    df : pandas.DataFrame
        Micro-batch dari Kafka Consumer.

    Returns
    -------
    validation_result
        Hasil validasi GX.
    """

    if df.empty:
        return None

    # ------------------------------------------------------------------------
    # 1. CREATE CONTEXT
    # ------------------------------------------------------------------------

    context = create_gx_context()

    # ------------------------------------------------------------------------
    # 2. DATA SOURCE
    # ------------------------------------------------------------------------

    data_source = context.data_sources.add_pandas(
        name="kafka_microbatch_source"
    )

    # ------------------------------------------------------------------------
    # 3. DATA ASSET
    # ------------------------------------------------------------------------

    data_asset = data_source.add_dataframe_asset(
        name="transactions_microbatch"
    )

    # ------------------------------------------------------------------------
    # 4. BATCH DEFINITION
    # ------------------------------------------------------------------------

    batch_definition = (
        data_asset.add_batch_definition_whole_dataframe(
            "microbatch"
        )
    )

    # ------------------------------------------------------------------------
    # 5. BATCH
    # ------------------------------------------------------------------------

    batch = batch_definition.get_batch(
        batch_parameters={
            "dataframe": df
        }
    )

    # ------------------------------------------------------------------------
    # 6. EXPECTATION SUITE
    # ------------------------------------------------------------------------

    suite = create_expectation_suite(context)

    # ------------------------------------------------------------------------
    # 7. VALIDATE
    # ------------------------------------------------------------------------

    validation_result = batch.validate(
        suite
    )

    return validation_result


# ============================================================================
# HASIL GX
# ============================================================================

def extract_gx_summary(validation_result):
    """
    Mengubah hasil GX menjadi summary sederhana.
    """

    if validation_result is None:
        return {
            "success": True,
            "total_expectations": 0,
            "passed_expectations": 0,
            "failed_expectations": 0,
        }

    results = validation_result["results"]

    total = len(results)

    passed = sum(
        1
        for result in results
        if result["success"]
    )

    failed = total - passed

    return {
        "success": validation_result["success"],
        "total_expectations": total,
        "passed_expectations": passed,
        "failed_expectations": failed,
    }


def save_gx_result(validation_result, batch_number):
    """
    Menyimpan hasil lengkap GX ke JSON.
    """

    if validation_result is None:
        return None

    timestamp = datetime.now().strftime(
        "%Y%m%d_%H%M%S_%f"
    )

    filename = (
        f"gx_validation_batch_{batch_number}_"
        f"{timestamp}.json"
    )

    filepath = os.path.join(
        REPORT_DIR,
        filename
    )

    # GX ValidationResult biasanya dapat dikonversi
    # menggunakan .to_json_dict().
    try:

        result_dict = validation_result.to_json_dict()

    except AttributeError:

        result_dict = dict(validation_result)

    with open(
        filepath,
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            result_dict,
            file,
            indent=4,
            default=str
        )

    return filepath


# ============================================================================
# ROW LEVEL VALIDATION
# ============================================================================

def validate_rows(df):
    """
    Melakukan pemeriksaan per baris untuk menentukan:

    VALID
    atau
    INVALID

    GX digunakan untuk validasi batch-level.

    Fungsi ini digunakan untuk routing record karena pipeline kita
    membutuhkan pemisahan setiap record ke:

        valid_transactions.csv
        transactions-dlq

    Returns
    -------
    valid_df
    invalid_df
    """

    if df.empty:

        return (
            pd.DataFrame(columns=df.columns),
            pd.DataFrame(columns=df.columns),
        )

    df = df.copy()

    invalid_reasons = []

    # ------------------------------------------------------------------------
    # DATE
    # ------------------------------------------------------------------------

    parsed_dates = pd.to_datetime(
        df["Date"],
        errors="coerce",
        dayfirst=False
    )

    date_invalid = parsed_dates.isna()

    # ------------------------------------------------------------------------
    # REQUIRED COLUMNS
    # ------------------------------------------------------------------------

    required_invalid = df[REQUIRED_COLUMNS].isnull().any(
        axis=1
    )

    # ------------------------------------------------------------------------
    # PRICE
    # ------------------------------------------------------------------------

    price_numeric = pd.to_numeric(
        df["Price"],
        errors="coerce"
    )

    price_invalid = (
        price_numeric.isna()
        | (price_numeric <= 0)
        | (price_numeric > 1000000)
    )

    # ------------------------------------------------------------------------
    # QUANTITY
    # ------------------------------------------------------------------------

    quantity_numeric = pd.to_numeric(
        df["Quantity"],
        errors="coerce"
    )

    quantity_invalid = (
        quantity_numeric.isna()
        | (quantity_numeric < 1)
        | (quantity_numeric > 10000)
    )

    # ------------------------------------------------------------------------
    # COUNTRY
    # ------------------------------------------------------------------------

    country_invalid = ~df["Country"].isin(
        VALID_COUNTRIES
    )

    # ------------------------------------------------------------------------
    # DUPLICATE
    # ------------------------------------------------------------------------

    duplicate_invalid = df.duplicated(
        subset=[
            "TransactionNo",
            "ProductNo"
        ],
        keep=False
    )

    # ------------------------------------------------------------------------
    # GABUNGKAN SEMUA ALASAN
    # ------------------------------------------------------------------------

    invalid_mask = (
        required_invalid
        | date_invalid
        | price_invalid
        | quantity_invalid
        | country_invalid
        | duplicate_invalid
    )

    # ------------------------------------------------------------------------
    # SIMPAN ALASAN INVALID
    # ------------------------------------------------------------------------

    def get_reason(index):

        reasons = []

        if required_invalid.loc[index]:
            reasons.append("missing_required_value")

        if date_invalid.loc[index]:
            reasons.append("invalid_date")

        if price_invalid.loc[index]:
            reasons.append("invalid_price")

        if quantity_invalid.loc[index]:
            reasons.append("invalid_quantity")

        if country_invalid.loc[index]:
            reasons.append("invalid_country")

        if duplicate_invalid.loc[index]:
            reasons.append("duplicate_transaction_product")

        return ";".join(reasons)

    df["validation_error"] = [
        get_reason(index)
        if invalid_mask.loc[index]
        else ""
        for index in df.index
    ]

    # ------------------------------------------------------------------------
    # VALID / INVALID
    # ------------------------------------------------------------------------

    valid_df = df[
        ~invalid_mask
    ].copy()

    invalid_df = df[
        invalid_mask
    ].copy()

    return valid_df, invalid_df


# ============================================================================
# FUNGSI UTAMA VALIDASI
# ============================================================================

def validate_microbatch(df, batch_number=0):
    """
    Pipeline lengkap validasi satu micro-batch.

    1. GX batch validation
    2. Row-level validation
    3. Simpan laporan
    4. Return valid dan invalid
    """

    if df.empty:

        return {
            "valid_df": df,
            "invalid_df": df,
            "gx_result": None,
            "gx_summary": {
                "success": True,
                "total_expectations": 0,
                "passed_expectations": 0,
                "failed_expectations": 0,
            },
        }

    # ------------------------------------------------------------------------
    # GX
    # ------------------------------------------------------------------------

    gx_result = run_gx_validation(df)

    gx_summary = extract_gx_summary(
        gx_result
    )

    # ------------------------------------------------------------------------
    # ROW LEVEL
    # ------------------------------------------------------------------------

    valid_df, invalid_df = validate_rows(df)

    # ------------------------------------------------------------------------
    # SAVE GX RESULT
    # ------------------------------------------------------------------------

    gx_report = save_gx_result(
        gx_result,
        batch_number
    )

    # ------------------------------------------------------------------------
    # RETURN
    # ------------------------------------------------------------------------

    return {
        "valid_df": valid_df,
        "invalid_df": invalid_df,
        "gx_result": gx_result,
        "gx_summary": gx_summary,
        "gx_report": gx_report,
    }