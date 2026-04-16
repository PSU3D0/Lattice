# /// script
# requires-python = ">=3.12"
# dependencies = ["openpyxl==3.1.5"]
# ///

from __future__ import annotations

from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
import argparse
import zipfile

from openpyxl import Workbook
from openpyxl.workbook.properties import CalcProperties

FIXED_ZIP_DT = (2024, 1, 1, 0, 0, 0)
FIXED_CREATED = datetime(2024, 1, 1, tzinfo=timezone.utc)
FIXED_MODIFIED = datetime(2024, 1, 1, tzinfo=timezone.utc)


def build_quote_workbook_bytes() -> bytes:
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Quote"

    workbook.properties.creator = "Lattice"
    workbook.properties.lastModifiedBy = "Lattice"
    workbook.properties.created = FIXED_CREATED
    workbook.properties.modified = FIXED_MODIFIED
    workbook.calculation = CalcProperties(calcMode="auto")

    sheet["A1"] = 0
    sheet["A2"] = 0
    sheet["A3"] = 0
    sheet["A4"] = "=A1*A2*(1-A3)"

    raw = BytesIO()
    workbook.save(raw)
    return repack_zip_deterministically(raw.getvalue())


def repack_zip_deterministically(payload: bytes) -> bytes:
    source = zipfile.ZipFile(BytesIO(payload), "r")
    out = BytesIO()
    with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_STORED) as zf:
        for name in sorted(source.namelist()):
            info = zipfile.ZipInfo(filename=name, date_time=FIXED_ZIP_DT)
            info.compress_type = zipfile.ZIP_STORED
            info.create_system = 3
            info.external_attr = 0o100644 << 16
            zf.writestr(info, source.read(name))
    return out.getvalue()


def main() -> None:
    parser = argparse.ArgumentParser(description="Provision deterministic quote-model XLSX asset")
    parser.add_argument(
        "--out",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "assets" / "quote_model.xlsx",
        help="Output XLSX path",
    )
    args = parser.parse_args()

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_bytes(build_quote_workbook_bytes())
    print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
