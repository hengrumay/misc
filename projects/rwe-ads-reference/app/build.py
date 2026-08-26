#!/usr/bin/env python3
"""
Build script for RWE ADS Automation app.

Builds the Vite frontend, outputs to static/, then ready for deployment.
Usage: python build.py
"""
import subprocess
import sys
from pathlib import Path
import shutil

APP_DIR = Path(__file__).parent
FRONTEND_DIR = APP_DIR / "frontend"
STATIC_DIR = APP_DIR / "static"
REPO_ROOT = APP_DIR.parent


def run(cmd: list[str], cwd: Path = None) -> int:
    """Run a command and return exit code."""
    print(f"[BUILD] {' '.join(cmd)}")
    return subprocess.call(cmd, cwd=cwd)


def vendor_config():
    """Make the app self-contained: copy demo.config.yaml + lib/config.py into
    the app so the deployed app (which ships only app/) can resolve names.
    Keeps demo.config.yaml as the single source of truth (this is a build copy)."""
    print("[BUILD] Vendoring demo.config.yaml + lib/config.py into app/")
    shutil.copy2(REPO_ROOT / "demo.config.yaml", APP_DIR / "demo.config.yaml")
    lib_dir = APP_DIR / "lib"
    lib_dir.mkdir(exist_ok=True)
    (lib_dir / "__init__.py").write_text("")
    shutil.copy2(REPO_ROOT / "lib" / "config.py", lib_dir / "config.py")


def main():
    print("[BUILD] RWE ADS Automation - Frontend Build")
    vendor_config()

    # Check if Node.js is available
    if run(["npm", "--version"]) != 0:
        print("[ERROR] npm not found. Install Node.js first.")
        return 1

    # Install dependencies. Prefer `npm ci` when a lockfile is present: it installs
    # the exact, pinned versions from package-lock.json (reproducible builds) and
    # fails if the lockfile is out of sync — the CI-grade choice. Fall back to
    # `npm install` only when there is no lockfile to pin against.
    if (FRONTEND_DIR / "package-lock.json").exists():
        install_cmd = ["npm", "ci"]
        print("[BUILD] Installing dependencies (npm ci — pinned from lockfile)...")
    else:
        install_cmd = ["npm", "install"]
        print("[BUILD] Installing dependencies (npm install — no lockfile found)...")
    if run(install_cmd, cwd=FRONTEND_DIR) != 0:
        print(f"[ERROR] {' '.join(install_cmd)} failed")
        return 1

    # Build with Vite
    print("[BUILD] Running Vite build...")
    if run(["npm", "run", "build"], cwd=FRONTEND_DIR) != 0:
        print("[ERROR] Vite build failed")
        return 1

    # Check output
    if STATIC_DIR.exists():
        index_html = STATIC_DIR / "index.html"
        if index_html.exists():
            print(f"[BUILD] ✓ Build complete: {STATIC_DIR}")
            # List the built artifacts so the operator can eyeball what will be
            # committed + deployed (the app serves exactly these files).
            print("[BUILD] Built assets:")
            for path in sorted(STATIC_DIR.rglob("*")):
                if path.is_file():
                    print(f"[BUILD]   {path.relative_to(STATIC_DIR)}  ({path.stat().st_size} bytes)")
            return 0
        else:
            print(f"[ERROR] index.html not found in {STATIC_DIR}")
            return 1
    else:
        print(f"[ERROR] static/ directory not created")
        return 1


if __name__ == "__main__":
    sys.exit(main())
