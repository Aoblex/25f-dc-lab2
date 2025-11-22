#!/bin/bash

# Check arguments
if [ "$#" -lt 2 ]; then
  echo "Usage: $0 <YEAR> <MONTH1> [MONTH2] [MONTH3] ..."
  echo "Example: $0 2025 01 02 03"
  exit 1
fi

# Read the first argument as YEAR
YEAR=$1
shift  # Remove the first argument so that $@ now contains only months

# The remaining arguments are months
MONTHS=("$@")

DATASET_DIR="datasets"

# Create target folder
mkdir -p ${DATASET_DIR}

# Loop through each month and download the data
for MONTH in "${MONTHS[@]}"
do
  URL="https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_${YEAR}-${MONTH}.parquet"
  FILENAME="${DATASET_DIR}/yellow_tripdata_${YEAR}-${MONTH}.parquet"
  INFOFILE="${DATASET_DIR}/yellow_tripdata_${YEAR}-${MONTH}_info.txt"

  # Skip if file already exists
  if [ -f "${FILENAME}" ]; then
    echo "File already exists: ${FILENAME}, skipping..."

    # 输出 schema 和行数到 INFOFILE
    python3 - <<EOF
import pyarrow.parquet as pq
pf = pq.ParquetFile("${FILENAME}")
with open("${INFOFILE}", "w") as f:
    f.write(f"File: ${FILENAME}\n")
    f.write(f"Number of rows: {pf.metadata.num_rows}\n")
    f.write("Schema:\n")
    f.write(str(pf.schema) + "\n")
EOF

    echo "Schema and row count written to ${INFOFILE}"
    continue
  fi

  echo "Downloading ${URL}..."
  curl -o "${FILENAME}" "${URL}"

  if [ $? -eq 0 ]; then
    echo "Successfully downloaded to ${FILENAME}"

    # 输出 schema 和行数到 INFOFILE
    python3 - <<EOF
import pyarrow.parquet as pq
pf = pq.ParquetFile("${FILENAME}")
with open("${INFOFILE}", "w") as f:
    f.write(f"File: ${FILENAME}\n")
    f.write(f"Number of rows: {pf.metadata.num_rows}\n")
    f.write("Schema:\n")
    f.write(str(pf.schema) + "\n")
EOF
    echo "Schema and row count written to ${INFOFILE}"
  else
    echo "Download failed: ${URL}"
  fi
done


echo "All downloads complete."
