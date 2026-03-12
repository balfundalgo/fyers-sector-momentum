"""
bundler.py
Balfund Trading Private Limited

Merges fyers_connect.py, fyers_token.py, strategy.py, gui.py
into bundled_main.py for PyInstaller.

Key: strips if __name__ == "__main__" blocks from all files
     except the last one (gui.py) so only the GUI entry point runs.
"""

from pathlib import Path

FILES_IN_ORDER = [
    "fyers_connect.py",
    "fyers_token.py",
    "strategy.py",
    "gui.py",          # LAST file - its __main__ block is kept
]

STRIP_PREFIXES = (
    "from fyers_token import",
    "from fyers_connect import",
    "from strategy import",
    "from __future__ import annotations",
)

OUTPUT_FILE = "bundled_main.py"


def strip_main_block(lines):
    """Remove if __name__ == '__main__': block and everything indented under it."""
    result = []
    in_main = False
    for line in lines:
        stripped = line.rstrip()
        # Detect start of __main__ block
        if stripped in ('if __name__ == "__main__":', "if __name__ == '__main__':"):
            in_main = True
            continue
        # If we are inside the __main__ block, skip indented lines
        if in_main:
            if stripped == "" or line.startswith("    ") or line.startswith("\t"):
                continue
            else:
                # Back to top-level code - exit main block
                in_main = False
        result.append(line)
    return result


def bundle():
    output_lines = []
    output_lines.append("from __future__ import annotations\n\n")
    output_lines.append('"""\nFyers Sector Momentum Strategy - Bundled Build\nBalfund Trading Private Limited\n"""\n\n')

    seen_imports = set()
    last_file = FILES_IN_ORDER[-1]

    for fname in FILES_IN_ORDER:
        path = Path(fname)
        if not path.exists():
            raise FileNotFoundError(f"Source file not found: {fname}")

        lines = path.read_text(encoding="utf-8").splitlines(keepends=True)

        # Strip __main__ block from all files except the last (gui.py)
        if fname != last_file:
            lines = strip_main_block(lines)

        output_lines.append(f"\n# {'='*70}\n# SOURCE: {fname}\n# {'='*70}\n\n")

        for line in lines:
            stripped = line.strip()
            if stripped.startswith("from __future__"):
                continue
            if any(stripped.startswith(p) for p in STRIP_PREFIXES):
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
