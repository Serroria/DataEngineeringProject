"""
==============================================================================
metrics.py

Menghitung metrik evaluasi pipeline:

1. Total records
2. Valid records
3. Invalid records
4. Validation rate
5. Detection rate
6. Processing latency
7. Throughput
8. GX success rate
==============================================================================
"""

import os
import pandas as pd


class MetricsTracker:

    def __init__(self):

        self.total_records = 0

        self.valid_records = 0

        self.invalid_records = 0

        self.total_processing_time = 0.0

        self.total_batches = 0

        self.gx_success_batches = 0

        self.gx_failed_expectations = 0

        self.batch_results = []


    # =========================================================================
    # RECORD BATCH
    # =========================================================================

    def record_batch(
        self,
        total,
        valid,
        invalid,
        processing_time,
        gx_success,
        gx_failed_expectations
    ):

        self.total_records += total

        self.valid_records += valid

        self.invalid_records += invalid

        self.total_processing_time += (
            processing_time
        )

        self.total_batches += 1

        if gx_success:
            self.gx_success_batches += 1

        self.gx_failed_expectations += (
            gx_failed_expectations
        )

        throughput = (
            total / processing_time
            if processing_time > 0
            else 0
        )

        self.batch_results.append({

            "batch_number":
                self.total_batches,

            "total_records":
                total,

            "valid_records":
                valid,

            "invalid_records":
                invalid,

            "processing_time_seconds":
                processing_time,

            "throughput_records_per_second":
                throughput,

            "gx_success":
                gx_success,

            "gx_failed_expectations":
                gx_failed_expectations,
        })


    # =========================================================================
    # VALIDATION RATE
    # =========================================================================

    def validation_rate(self):

        if self.total_records == 0:
            return 0

        return (
            self.valid_records
            / self.total_records
        ) * 100


    # =========================================================================
    # DETECTION RATE
    # =========================================================================

    def detection_rate(self):

        if self.total_records == 0:
            return 0

        return (
            self.invalid_records
            / self.total_records
        ) * 100


    # =========================================================================
    # AVERAGE LATENCY
    # =========================================================================

    def average_latency(self):

        if self.total_batches == 0:
            return 0

        return (
            self.total_processing_time
            / self.total_batches
        )


    # =========================================================================
    # THROUGHPUT
    # =========================================================================

    def throughput(self):

        if self.total_processing_time == 0:
            return 0

        return (
            self.total_records
            / self.total_processing_time
        )


    # =========================================================================
    # GX SUCCESS RATE
    # =========================================================================

    def gx_success_rate(self):

        if self.total_batches == 0:
            return 0

        return (
            self.gx_success_batches
            / self.total_batches
        ) * 100


    # =========================================================================
    # SUMMARY
    # =========================================================================

    def get_summary(self):

        return {

            "total_batches":
                self.total_batches,

            "total_records":
                self.total_records,

            "valid_records":
                self.valid_records,

            "invalid_records":
                self.invalid_records,

            "validation_rate_percent":
                self.validation_rate(),

            "detection_rate_percent":
                self.detection_rate(),

            "average_latency_seconds":
                self.average_latency(),

            "throughput_records_per_second":
                self.throughput(),

            "gx_success_rate_percent":
                self.gx_success_rate(),

            "gx_failed_expectations":
                self.gx_failed_expectations,
        }


    # =========================================================================
    # PRINT SUMMARY
    # =========================================================================

    def print_summary(self):

        summary = self.get_summary()

        print(
            f"Total batch              : "
            f"{summary['total_batches']}"
        )

        print(
            f"Total records            : "
            f"{summary['total_records']}"
        )

        print(
            f"Valid records            : "
            f"{summary['valid_records']}"
        )

        print(
            f"Invalid records          : "
            f"{summary['invalid_records']}"
        )

        print(
            f"Validation rate         : "
            f"{summary['validation_rate_percent']:.2f}%"
        )

        print(
            f"Detection rate          : "
            f"{summary['detection_rate_percent']:.2f}%"
        )

        print(
            f"Average latency         : "
            f"{summary['average_latency_seconds']:.4f} sec"
        )

        print(
            f"Throughput              : "
            f"{summary['throughput_records_per_second']:.2f} records/sec"
        )

        print(
            f"GX success rate         : "
            f"{summary['gx_success_rate_percent']:.2f}%"
        )

        print(
            f"GX failed expectations  : "
            f"{summary['gx_failed_expectations']}"
        )


    # =========================================================================
    # SAVE CSV
    # =========================================================================

    def save_batch_results(
        self,
        filepath="data/output/quality_reports/quality_summary.csv"
    ):

        if not self.batch_results:

            return

        os.makedirs(
            os.path.dirname(filepath),
            exist_ok=True
        )

        df = pd.DataFrame(
            self.batch_results
        )

        df.to_csv(
            filepath,
            index=False
        )

        print(
            f"Metrics disimpan ke: {filepath}"
        )