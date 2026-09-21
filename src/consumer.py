"""
==============================================================================
consumer.py

Kafka Consumer
Sistem Monitoring dan Validasi Kualitas Data Otomatis

Alur:

Kafka transactions
        ↓
Consumer
        ↓
Micro-batch
        ↓
Great Expectations
        ↓
Row Validation
   ┌────┴────┐
   ↓         ↓
 VALID     INVALID
   ↓         ↓
CSV       Kafka DLQ
           +
         error log
==============================================================================
"""

import json
import os
import time
from datetime import datetime

import pandas as pd
from kafka import KafkaConsumer, KafkaProducer

from .validation import validate_microbatch
from .metrics import MetricsTracker

# ============================================================================
# KONFIGURASI 
# ============================================================================

KAFKA_SERVER = "localhost:9092"

INPUT_TOPIC = "transactions"

DLQ_TOPIC = "transactions-dlq"

GROUP_ID = "quality-validation-consumer"

BATCH_SIZE = 100

BATCH_TIMEOUT_SECONDS = 5

OUTPUT_DIR = "data/output"

VALID_DIR = os.path.join(
    OUTPUT_DIR,
    "valid"
)

ERROR_DIR = os.path.join(
    OUTPUT_DIR,
    "error_logs"
)

os.makedirs(
    VALID_DIR,
    exist_ok=True
)

os.makedirs(
    ERROR_DIR,
    exist_ok=True
)


VALID_FILE = os.path.join(
    VALID_DIR,
    "valid_transactions.csv"
)

INVALID_FILE = os.path.join(
    ERROR_DIR,
    "invalid_transactions.csv"
)


# ============================================================================
# KAFKA CONSUMER
# ============================================================================
def safe_deserialize(value):
    """Deserialize value Kafka dengan aman."""
    if value is None:
        return None
    try:
        return json.loads(value.decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        # Kembalikan dict khusus supaya tidak crash
        return {
            "_raw": value.decode("utf-8", errors="replace"),
            "_error": f"deserialization_failed: {e}",
        }
    
consumer = KafkaConsumer(
    INPUT_TOPIC,

    bootstrap_servers=KAFKA_SERVER,

    group_id=GROUP_ID,

    auto_offset_reset="earliest",

    enable_auto_commit=True,

   value_deserializer=safe_deserialize,
   
    consumer_timeout_ms=1000,
)


# ============================================================================
# KAFKA DLQ PRODUCER
# ============================================================================

dlq_producer = KafkaProducer(

    bootstrap_servers=KAFKA_SERVER,

    value_serializer=lambda value:
        json.dumps(
            value,
            default=str
        ).encode("utf-8")
)


# ============================================================================
# METRICS
# ============================================================================

metrics = MetricsTracker()


# ============================================================================
# SIMPAN DATA VALID
# ============================================================================

def save_valid_data(df):
    """
    Menyimpan record valid ke CSV.
    """

    if df.empty:
        return

    df_to_save = df.copy()

    # validation_error tidak diperlukan pada valid data
    if "validation_error" in df_to_save.columns:
        df_to_save = df_to_save.drop(
            columns=["validation_error"]
        )

    file_exists = os.path.exists(
        VALID_FILE
    )

    df_to_save.to_csv(
        VALID_FILE,
        mode="a",
        index=False,
        header=not file_exists
    )


# ============================================================================
# SIMPAN DATA INVALID
# ============================================================================

def save_invalid_data(df):
    """
    Menyimpan invalid records ke error log.
    """

    if df.empty:
        return

    file_exists = os.path.exists(
        INVALID_FILE
    )

    df.to_csv(
        INVALID_FILE,
        mode="a",
        index=False,
        header=not file_exists
    )


# ============================================================================
# KIRIM KE DLQ
# ============================================================================

def send_to_dlq(df):
    """
    Mengirim record invalid ke Kafka Dead Letter Queue.
    """

    if df.empty:
        return

    for _, row in df.iterrows():

        record = row.to_dict()

        record["dlq_timestamp"] = (
            datetime.now().isoformat()
        )

        record["dlq_reason"] = record.get(
            "validation_error",
            "unknown_error"
        )

        dlq_producer.send(
            DLQ_TOPIC,
            value=record
        )

    dlq_producer.flush()


# ============================================================================
# PROSES MICRO-BATCH
# ============================================================================

def process_microbatch(records, batch_number):
    """
    Memproses satu micro-batch.
    """

    if not records:
        return

       # Filter record yang gagal deserialize
    clean_records = [r for r in records if r and "_error" not in r]
    dropped = len(records) - len(clean_records)
    if dropped:
        print(f"[WARN] {dropped} record gagal deserialize, dilewati.")

    if not clean_records:
        return

    records = clean_records

    start_time = time.perf_counter()

    # ------------------------------------------------------------------------
    # RECORD → DATAFRAME
    # ------------------------------------------------------------------------

    df = pd.DataFrame(records)

    total_records = len(df)

    print()
    print("=" * 70)
    print(
        f"MICRO-BATCH #{batch_number}"
    )
    print(
        f"Jumlah record : {total_records}"
    )
    print("=" * 70)

    # ------------------------------------------------------------------------
    # VALIDATION
    # ------------------------------------------------------------------------

    result = validate_microbatch(
        df,
        batch_number=batch_number
    )

    valid_df = result["valid_df"]

    invalid_df = result["invalid_df"]

    gx_summary = result["gx_summary"]

    # ------------------------------------------------------------------------
    # OUTPUT VALID
    # ------------------------------------------------------------------------

    save_valid_data(
        valid_df
    )

    # ------------------------------------------------------------------------
    # OUTPUT INVALID
    # ------------------------------------------------------------------------

    save_invalid_data(
        invalid_df
    )

    # ------------------------------------------------------------------------
    # DLQ
    # ------------------------------------------------------------------------

    send_to_dlq(
        invalid_df
    )

    # ------------------------------------------------------------------------
    # METRICS
    # ------------------------------------------------------------------------

    processing_time = (
        time.perf_counter()
        - start_time
    )

    metrics.record_batch(
        total=total_records,
        valid=len(valid_df),
        invalid=len(invalid_df),
        processing_time=processing_time,
        gx_success=gx_summary["success"],
        gx_failed_expectations=(
            gx_summary["failed_expectations"]
        )
    )
    

    # ------------------------------------------------------------------------
    # TERMINAL OUTPUT
    # ------------------------------------------------------------------------

    print(
        f"GX success            : "
        f"{gx_summary['success']}"
    )

    print(
        f"Expectation total     : "
        f"{gx_summary['total_expectations']}"
    )

    print(
        f"Expectation passed    : "
        f"{gx_summary['passed_expectations']}"
    )

    print(
        f"Expectation failed    : "
        f"{gx_summary['failed_expectations']}"
    )

    print(
        f"Valid records         : "
        f"{len(valid_df)}"
    )

    print(
        f"Invalid records       : "
        f"{len(invalid_df)}"
    )

    print(
        f"Processing time       : "
        f"{processing_time:.4f} seconds"
    )

    print(
        f"Throughput            : "
        f"{total_records / processing_time:.2f} "
        f"records/sec"
    )


# ============================================================================
# MAIN
# ============================================================================

def main():

    print()
    print("=" * 70)
    print("KAFKA DATA QUALITY CONSUMER")
    print("=" * 70)

    print(
        f"Kafka server : {KAFKA_SERVER}"
    )

    print(
        f"Input topic  : {INPUT_TOPIC}"
    )

    print(
        f"DLQ topic    : {DLQ_TOPIC}"
    )

    print(
        f"Batch size   : {BATCH_SIZE}"
    )

    print()
    print("Menunggu data dari Kafka...")
    print("Tekan CTRL+C untuk menghentikan.")
    print()

    batch = []

    batch_number = 1

    last_batch_time = time.time()

    try:

        while True:

            # --------------------------------------------------------------
            # AMBIL RECORD
            # --------------------------------------------------------------

            message_pack = consumer.poll(
                timeout_ms=1000,
                max_records=BATCH_SIZE
            )

            for _, messages in message_pack.items():

                for message in messages:

                    batch.append(
                        message.value
                    )

            # --------------------------------------------------------------
            # PROSES JIKA:
            #
            # 1. batch sudah mencapai BATCH_SIZE
            # ATAU
            # 2. timeout tercapai
            # --------------------------------------------------------------

            elapsed = (
                time.time()
                - last_batch_time
            )

            if (
                len(batch) >= BATCH_SIZE
                or (
                    batch
                    and elapsed >= BATCH_TIMEOUT_SECONDS
                )
            ):

                process_microbatch(
                    batch,
                    batch_number
                )

                batch = []

                batch_number += 1

                last_batch_time = time.time()

    except KeyboardInterrupt:

        print()
        print(
            "Consumer dihentikan oleh user."
        )

        # --------------------------------------------------------------
        # PROSES SISA DATA
        # --------------------------------------------------------------

        if batch:

            print(
                f"Memproses sisa "
                f"{len(batch)} record..."
            )

            process_microbatch(
                batch,
                batch_number
            )

    finally:

        print()
        print("=" * 70)
        print("FINAL METRICS")
        print("=" * 70)

        metrics.print_summary()
        metrics.save_batch_results()

        # --------------------------------------------------------------
        # CLOSE
        # --------------------------------------------------------------

        consumer.close()

        dlq_producer.close()


if __name__ == "__main__":
    main()