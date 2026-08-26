"""Generate a validated work instruction (SOP) markdown document.

Reads demo.config.yaml and generates docs/VALIDATED_WORK_INSTRUCTION.md
(marked as DRAFT, requiring formal governance sign-off).

Usage:
  python generate_wi.py
  python generate_wi.py --config path/to/custom.yaml
"""
from __future__ import annotations

# Serverless spark_python_task runs this file via exec() with no __file__ in
# globals; recover it from the frame so downstream Path(__file__) works.
try:
    __file__
except NameError:  # pragma: no cover
    import inspect as _inspect
    __file__ = _inspect.getfile(_inspect.currentframe())

import argparse
import sys
from pathlib import Path

# Make lib importable
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

try:
    import yaml
except ImportError:
    print("PyYAML required. Install: pip install pyyaml")
    sys.exit(1)


def main(config_path: str | None = None, output_path: str | None = None) -> None:
    """Generate work instruction markdown.

    Args:
        config_path: Path to demo.config.yaml (or discover via lib.config)
        output_path: Path to output markdown file (default: docs/VALIDATED_WORK_INSTRUCTION.md)
    """
    from lib.config import cfg

    # Load config
    c = cfg()

    # Import work_instruction module (local to this file's directory)
    from waves.wave5_serving_audit_app.work_instruction import render_work_instruction

    # Render markdown
    md_content = render_work_instruction(
        config_dict=c._d,
        poc_studies=c.poc_studies,
    )

    # Output file (relative to repo root)
    if output_path is None:
        output_file = Path(__file__).resolve().parents[2] / "docs" / "VALIDATED_WORK_INSTRUCTION.md"
    else:
        output_file = Path(output_path)

    # Ensure output directory exists
    output_file.parent.mkdir(parents=True, exist_ok=True)

    # Write
    with open(output_file, "w") as f:
        f.write(md_content)

    print(f"[OK] Work instruction generated: {output_file}")
    print(f"     Status: DRAFT (requires formal governance sign-off)")
    print(f"     Lines: {len(md_content.splitlines())}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate validated work instruction SOP")
    parser.add_argument("--config", help="Path to demo.config.yaml (auto-discovered if omitted)")
    parser.add_argument("--output", help="Output markdown path (default: docs/VALIDATED_WORK_INSTRUCTION.md)")
    args = parser.parse_args()

    main(config_path=args.config, output_path=args.output)
