"""
==============================================================================
producer.py

Membaca Sales Transaction v.4a.csv
kemudian mensimulasikan real-time streaming melalui Apache Kafka.

Data:

CSV
 ↓
Pandas
 ↓
JSON
 ↓
Kafka topic: transactions

Mode normal:
data dikirim sesuai dataset.

Mode anomaly:
beberapa record sengaja dimodifikasi untuk demonstrasi
data quality validation.
==============================================================================
"""

import json
import random
import time

import pandas as pd
from kafka import KafkaProducer


# ============================================================================
# KONFIGURASI
# ============================================================================

KAFKA_SERVER = "localhost:9092"

TOPIC = "transactions"

CSV_PATH = (
    "data/raw/Sales Transaction v.4a.csv"
)

# Untuk testing pertama:
# gunakan 100.
#
# Setelah berhasil:
# ubah menjadi None untuk semua data.
MAX_RECORDS = 100

# Delay simulasi real-time
DELAY_SECONDS = 0.05

# Aktifkan anomaly injection
INJECT_ANOMALIES = True


# ============================================================================
# PRODUCER
# ============================================================================

producer = KafkaProducer(

    bootstrap_servers=KAFKA_SERVER,

    value_serializer=lambda value:
        json.dumps(
            value,
            default=str
        ).encode("utf-8")
)


# ============================================================================
# ANOMALY INJECTION
# ============================================================================

def inject_anomaly(data, index):
    """
    Membuat beberapa data invalid secara sengaja.

    Tujuan:
    mendemonstrasikan kemampuan sistem mendeteksi
    masalah kualitas data.
    """

    # --------------------------------------------------------------
    # Setiap 25 record → anomaly
    # --------------------------------------------------------------

    if index % 25 != 0:

        return data

    anomaly_type = random.choice([

        "missing_price",

        "negative_quantity",

        "invalid_country",

        "invalid_date",

        "duplicate"

    ])


    # --------------------------------------------------------------
    # MISSING PRICE
    # --------------------------------------------------------------

    if anomaly_type == "missing_price":

        data["Price"] = None


    # --------------------------------------------------------------
    # NEGATIVE QUANTITY
    # --------------------------------------------------------------

    elif anomaly_type == "negative_quantity":

        data["Quantity"] = -99999


    # --------------------------------------------------------------
    # INVALID COUNTRY
    # --------------------------------------------------------------

    elif anomaly_type == "invalid_country":

        data["Country"] = "Invalid Country"


    # --------------------------------------------------------------
    # INVALID DATE
    # --------------------------------------------------------------

    elif anomaly_type == "invalid_date":

        data["Date"] = "INVALID_DATE"


    # --------------------------------------------------------------
    # DUPLICATE
    # --------------------------------------------------------------

    elif anomaly_type == "duplicate":

        # Menandai record sebagai duplikasi.
        #
        # Untuk benar-benar menghasilkan composite-key duplicate,
        # kita menggunakan TransactionNo/ProductNo yang sama.
        #
        # Karena record asli tidak kita simpan di sini,
        # cukup gunakan nilai yang sama pada beberapa record.
        # data["TransactionNo"] = "DUPLICATE_TEST"
        # data["ProductNo"] = "DUPLICATE_PRODUCT"
        if previous_record is not None:
            data["TransactionNo"] = (
                previous_record("TransactionNo")
            )
            data["ProductNo"] = (
                previous_record("ProductNo")
            )


    data["_injected_anomaly"] = anomaly_type

    return data


# ============================================================================
# MAIN
# ============================================================================

def main():

    print()
    print("=" * 70)
    print("KAFKA PRODUCER")
    print("=" * 70)

    print(
        f"CSV       : {CSV_PATH}"
    )

    print(
        f"Kafka     : {KAFKA_SERVER}"
    )

    print(
        f"Topic     : {TOPIC}"
    )

    print(
        f"Max data  : {MAX_RECORDS}"
    )

    print(
        f"Anomaly   : {INJECT_ANOMALIES}"
    )

    print()


    # ------------------------------------------------------------------------
    # LOAD CSV
    # ------------------------------------------------------------------------

    df = pd.read_csv(
        CSV_PATH
    )

    print(
        f"Dataset memiliki "
        f"{len(df):,} records."
    )


    # ------------------------------------------------------------------------
    # LIMIT UNTUK TESTING
    # ------------------------------------------------------------------------

    if MAX_RECORDS is not None:

        df = df.head(
            MAX_RECORDS
        ).copy()

        print(
            f"Testing menggunakan "
            f"{len(df):,} records."
        )


    # ------------------------------------------------------------------------
    # STREAM
    # ------------------------------------------------------------------------
    previous_record = None
    for index, row in df.iterrows():

        data = row.to_dict()

        # NaN → None supaya JSON valid
        data = {

            key:
                None
                if pd.isna(value)
                else value

            for key, value in data.items()
        }


        # --------------------------------------------------------------------
        # ANOMALY
        # --------------------------------------------------------------------

        if INJECT_ANOMALIES:

            data = inject_anomaly(
                data,
                index,
                previous_record
            )


        # --------------------------------------------------------------------
        # SEND
        # --------------------------------------------------------------------

        producer.send(
            TOPIC,
            value=data
        )
        previous_record = data.copy()

        time.sleep(DELAY_SECONDS)

        print(
            f"[{index + 1}/{len(df)}] "
            f"TransactionNo="
            f"{data.get('TransactionNo')} "
            f"Country="
            f"{data.get('Country')}"
        )


        # --------------------------------------------------------------------
        # SIMULASI REAL-TIME
        # --------------------------------------------------------------------

        time.sleep(
            DELAY_SECONDS
        )


    producer.flush()

    producer.close()


    print()
    print(
        "Streaming selesai."
    )


# ============================================================================
# ENTRY POINT
# ============================================================================

if __name__ == "__main__":

    main()