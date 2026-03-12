"""
bundler.py
Balfund Trading Private Limited

Merges fyers_connect.py, fyers_token.py, strategy.py, gui.py
into bundled_main.py for PyInstaller to build into a standalone EXE.

Run: python bundler.py
"""

from pathlib import Path

# Files merged in order - dependencies first, GUI last
FILES_IN_ORDER = [
    "fyers_connect.py",
    "fyers_token.py",
    "strategy.py",
    "gui.py",
]

STRIP_PREFIXES = (
    "from fyers_token import",
    "from fyers_connect import",
    "from strategy import",
    "from __future__ import annotations",
)

OUTPUT_FILE = "bundled_main.py"


def bundle():
    output_lines = []
    output_lines.append("from __future__ import annotations\n\n")
    output_lines.append('"""\nFyers Sector Momentum Strategy - Bundled Build\nBalfund Trading Private Limited\n"""\n\n')

    seen_imports = set()

    for fname in FILES_IN_ORDER:
        path = Path(fname)
        if not path.exists():
            raise FileNotFoundError(f"Source file not found: {fname}")

        lines = path.read_text(encoding="utf-8").splitlines(keepends=True)
        output_lines.append(f"\n# {'='*70}\n# SOURCE: {fname}\n# {'='*70}\n\n")

        for line in lines:
            stripped = line.strip()
            if stripped.startswith("from __future__"):
                continue
            skip = any(stripped.startswith(p) for p in STRIP_PREFIXES)
            if skip:
                continue
            if stripped.startswith(("import ", "from ")) and stripped in seen_imports:
                continue
            if stripped.startswith(("import ", "from ")):
                seen_imports.add(stripped)
            output_lines.append(line)

    Path(OUTPUT_FILE).write_text("".join(output_lines), encoding="utf-8")
    size = Path(OUTPUT_FILE).stat().st_size
    print(f"[BUNDLER] OK - Created {OUTPUT_FILE} ({size:,} bytes)")
    print(f"[BUNDLER] Merged: {', '.join(FILES_IN_ORDER)}")


if __name__ == "__main__":
    bundle()
