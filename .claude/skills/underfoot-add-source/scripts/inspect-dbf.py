#!/usr/bin/env python3
"""
Inspect a shapefile's .dbf to show unique values of a join column and
their associated field values.

Usage:
    python inspect-dbf.py path/to/file.dbf [JOIN_COLUMN]

JOIN_COLUMN defaults to UNIT. Shows all unique values with the other
fields, useful for:
  - Confirming the join column name and its values
  - Seeing what columns are available to map to units.csv fields
  - Getting a list of unit codes to build units.csv from
"""

import csv
import struct
import sys


def read_dbf(path):
    with open(path, 'rb') as f:
        f.read(4)  # version + date
        num_records = struct.unpack('<I', f.read(4))[0]
        header_size = struct.unpack('<H', f.read(2))[0]
        f.read(22)  # reserved

        fields = []
        while True:
            field_data = f.read(32)
            if not field_data or field_data[0] == 0x0D:
                break
            name = field_data[:11].rstrip(b'\x00').decode('latin-1')
            length = field_data[16]
            fields.append((name, length))

        f.seek(header_size)
        rows = []
        for _ in range(num_records):
            f.read(1)  # deletion flag
            row = {}
            for name, length in fields:
                row[name] = f.read(length).decode('latin-1', errors='ignore').strip()
            rows.append(row)

    return fields, rows


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    dbf_path = sys.argv[1]
    join_col = sys.argv[2] if len(sys.argv) > 2 else 'UNIT'

    fields, rows = read_dbf(dbf_path)
    field_names = [f[0] for f in fields]

    print(f"Fields: {field_names}\n")

    seen = {}
    for row in rows:
        key = row.get(join_col, '')
        if key and key not in seen:
            seen[key] = row

    print(f"Unique {join_col} values ({len(seen)}):")
    writer = csv.writer(sys.stdout)
    writer.writerow([join_col] + [n for n in field_names if n != join_col
                                  and n not in ('Shape_Leng', 'Shape_Area')])
    for key, row in sorted(seen.items()):
        writer.writerow(
            [key] + [row[n] for n in field_names if n != join_col
                     and n not in ('Shape_Leng', 'Shape_Area')]
        )


if __name__ == '__main__':
    main()
