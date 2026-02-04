#!/usr/bin/env bash
set -euo pipefail

DATA_DIR="data"
RESULTS_DIR="results"
FILE_COUNT="{{ file_count }}"
FILE_MB="${FILE_MB:-5}"
HASH_METHOD="{{ hash_method }}"
HASH_DELAY_SECS="${HASH_DELAY_SECS:-1}"

if ! [[ "$FILE_COUNT" =~ ^[0-9]+$ ]] || [ "$FILE_COUNT" -le 0 ]; then
  echo "FILE_COUNT must be a positive integer" >&2
  exit 1
fi
if ! [[ "$FILE_MB" =~ ^[0-9]+$ ]] || [ "$FILE_MB" -le 0 ]; then
  echo "FILE_MB must be a positive integer" >&2
  exit 1
fi

case "$HASH_METHOD" in
  sha256)
    if command -v sha256sum >/dev/null 2>&1; then
      hash_cmd=(sha256sum)
    elif command -v shasum >/dev/null 2>&1; then
      hash_cmd=(shasum -a 256)
    else
      echo "sha256sum or shasum not available on PATH" >&2
      exit 1
    fi
    ;;
  sha1)
    if command -v sha1sum >/dev/null 2>&1; then
      hash_cmd=(sha1sum)
    elif command -v shasum >/dev/null 2>&1; then
      hash_cmd=(shasum -a 1)
    else
      echo "sha1sum or shasum not available on PATH" >&2
      exit 1
    fi
    ;;
  md5)
    if command -v md5sum >/dev/null 2>&1; then
      hash_cmd=(md5sum)
    elif command -v md5 >/dev/null 2>&1; then
      hash_cmd=(md5)
    else
      echo "md5sum or md5 not available on PATH" >&2
      exit 1
    fi
    ;;
  *)
    echo "unsupported HASH_METHOD: $HASH_METHOD" >&2
    exit 1
    ;;
esac

mkdir -p "$DATA_DIR" "$RESULTS_DIR"
cp "inputs.txt" "$RESULTS_DIR/inputs.txt"

for i in $(seq -w 1 "$FILE_COUNT"); do
  out="$DATA_DIR/random-$i.bin"
  dd if=/dev/urandom of="$out" bs=1048576 count="$FILE_MB"
done

hash_file="$RESULTS_DIR/hashes.${HASH_METHOD}"
: > "$hash_file"

printf "hash run on %s at %s\n" "$(hostname)" "$(date '+%Y-%m-%d %H:%M:%S')" > "$RESULTS_DIR/summary.txt"
printf "hash_method: %s\n" "$HASH_METHOD" >> "$RESULTS_DIR/summary.txt"
printf "files: %s\n" "$FILE_COUNT" >> "$RESULTS_DIR/summary.txt"

shopt -s nullglob
files=("$DATA_DIR"/random-*.bin)
files_count=$(ls -1 "$DATA_DIR"/random-*.bin 2>/dev/null | wc -l | tr -d ' ')
if (( files_count == 0 )); then
  echo "no data files generated" >&2
  exit 1
fi

total_bytes=0
for file in "${files[@]}"; do
  size=$(wc -c < "$file")
  total_bytes=$((total_bytes + size))
  printf "hashing %s (%s bytes)\n" "$file" "$size" >> "$RESULTS_DIR/summary.txt"
  "${hash_cmd[@]}" "$file" >> "$hash_file"
  sleep "$HASH_DELAY_SECS"
done

printf "total_bytes: %s\n" "$total_bytes" >> "$RESULTS_DIR/summary.txt"
