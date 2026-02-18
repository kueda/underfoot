#!/usr/bin/env python3
"""
Generate a units.csv scaffold from a shapefile's .dbf.

Usage:
    python scaffold-units-csv.py path/to/file.dbf OUTPUT.csv \\
        --code UNIT --title Descriptio --span Age

Writes a CSV with columns: code, title, description, lithology, span
The description and lithology columns are left blank — fill them in from
the publication's pamphlet PDF using pdftotext:

    pdftotext -layout pamphlet.pdf - | less

Find the "Description of Map Units" section and transcribe descriptions
into the CSV. Do not leave the description column blank in the final file.
"""

import argparse
import csv
import struct
import sys


def read_dbf(path):
    with open(path, 'rb') as f:
        f.read(4)
        num_records = struct.unpack('<I', f.read(4))[0]
        header_size = struct.unpack('<H', f.read(2))[0]
        f.read(22)

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
            f.read(1)
            row = {}
            for name, length in fields:
                row[name] = f.read(length).decode('latin-1', errors='ignore').strip()
            rows.append(row)

    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('dbf', help='Path to .dbf file')
    parser.add_argument('output', help='Output CSV path')
    parser.add_argument('--code', default='UNIT',
                        help='DBF column to use as unit code (default: UNIT)')
    parser.add_argument('--title', default='Descriptio',
                        help='DBF column to use as title (default: Descriptio)')
    parser.add_argument('--span', default='Age',
                        help='DBF column to use as span/age (default: Age)')
    args = parser.parse_args()

    rows = read_dbf(args.dbf)

    seen = {}
    for row in rows:
        key = row.get(args.code, '')
        if key and key not in seen:
            seen[key] = row

    with open(args.output, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['code', 'title', 'description', 'lithology', 'span'])
        for key, row in sorted(seen.items()):
            writer.writerow([
                key,
                row.get(args.title, ''),
                '',   # fill from pamphlet
                '',   # inferred at run time
                row.get(args.span, ''),
            ])

    print(f"Wrote {len(seen)} units to {args.output}", file=sys.stderr)
    print("Next: populate the description column from the pamphlet PDF.", file=sys.stderr)
    print("  pdftotext -layout pamphlet.pdf - | less", file=sys.stderr)


if __name__ == '__main__':
    main()
