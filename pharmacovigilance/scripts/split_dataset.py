"""
split_dataset.py
=================
Splits the full FAERS CSV (~528,000 rows) into smaller chunk files, each
with the header repeated, so they can be dropped one by one into
data/input during the streaming demo.

Usage:
    python split_dataset.py --input data/faers_full.csv --output-dir data/input --chunk-size 5000

"""

import argparse
import csv
import os


def split_csv(input_path, output_dir, chunk_size):
    os.makedirs(output_dir, exist_ok=True)

    with open(input_path, "r", newline="", encoding="utf-8") as infile:
        reader = csv.reader(infile)
        header = next(reader)

        chunk_index = 0
        row_buffer = []

        def flush_chunk(buffer, index):
            if not buffer:
                return
            chunk_path = os.path.join(output_dir, f"chunk_{index:04d}.csv")
            with open(chunk_path, "w", newline="", encoding="utf-8") as outfile:
                writer = csv.writer(outfile)
                writer.writerow(header)
                writer.writerows(buffer)
            print(f"[OK] Wrote {len(buffer)} rows -> {chunk_path}")

        for row in reader:
            row_buffer.append(row)
            if len(row_buffer) >= chunk_size:
                chunk_index += 1
                flush_chunk(row_buffer, chunk_index)
                row_buffer = []

        # Flush any remaining rows into a final chunk
        if row_buffer:
            chunk_index += 1
            flush_chunk(row_buffer, chunk_index)

    print(f"[INFO] Done. {chunk_index} chunk file(s) written to '{output_dir}'.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Split the FAERS CSV into demo chunks.")
    parser.add_argument("--input", required=True, help="Path to the full FAERS CSV file.")
    parser.add_argument("--output-dir", default="data/input_chunks",
                         help="Directory where chunk files will be written "
                              "(NOT data/input directly, to avoid Spark picking "
                              "them all up at once).")
    parser.add_argument("--chunk-size", type=int, default=5000,
                         help="Number of data rows per chunk (default: 5000).")
    args = parser.parse_args()

    split_csv(args.input, args.output_dir, args.chunk_size)