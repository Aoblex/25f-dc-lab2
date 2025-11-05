#!/usr/bin/env python3
import os
import argparse
import pyarrow.parquet as pq
import pandas as pd


def process_parquet_file(
    input_path: str,
    output_path: str,
    sample_rows: int = 1000,
    batch_size: int = 100
):
    """Read a Parquet file in batches, collect the first N rows, and write to a new file."""
    parquet_file = pq.ParquetFile(input_path)
    batches = parquet_file.iter_batches(batch_size=batch_size)

    total_rows = 0
    collected_batches = []

    for batch in batches:
        df_batch = batch.to_pandas()
        needed = sample_rows - total_rows
        if needed <= 0:
            break
        df_needed = df_batch.head(needed)
        collected_batches.append(df_needed)
        total_rows += len(df_needed)
        if total_rows >= sample_rows:
            break

    df_sample = pd.concat(collected_batches, ignore_index=True)
    df_sample.to_parquet(output_path, index=False)
    print(f"{os.path.basename(input_path)} -> {output_path} ({len(df_sample)} rows)")


def main():
    parser = argparse.ArgumentParser(
        description="Read Parquet files in batches, keep only the first N rows, and save them to another directory."
    )
    parser.add_argument("--input", "-i", type=str, default="./datasets", help="Input directory containing Parquet files")
    parser.add_argument("--output", "-o", type=str, default="./datasets/sample", help="Output directory for sample files")
    parser.add_argument("--rows", "-r", type=int, default=100, help="Number of rows to keep from each file")
    parser.add_argument("--batch-size", "-b", type=int, default=1000, help="Batch size for reading (controls memory usage)")

    args = parser.parse_args()

    os.makedirs(args.output, exist_ok=True)
    files = [f for f in os.listdir(args.input) if f.endswith(".parquet")]

    if not files:
        print("No Parquet files found.")
        return

    print(f"Found {len(files)} Parquet files. Processing...")

    for filename in files:
        input_path = os.path.join(args.input, filename)
        output_path = os.path.join(args.output, filename)
        process_parquet_file(input_path, output_path, sample_rows=args.rows, batch_size=args.batch_size)

    print("All files processed.")


if __name__ == "__main__":
    main()