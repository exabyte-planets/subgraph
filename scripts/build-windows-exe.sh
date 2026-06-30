#!/usr/bin/env bash
#
# Mirror of .github/workflows/release-windows.yml — builds the standalone
# subgraph executable locally with the same commands CI uses, so you can
# reproduce/debug the bundle without pushing a tag.
#
# NOTE: PyInstaller cannot cross-compile. Running this on macOS/Linux produces a
# binary for *that* OS; only running it on Windows (e.g. Git Bash / WSL is NOT
# enough — it must be native Windows) yields subgraph.exe. It's wired up to run
# anywhere mainly so the build can be exercised before it hits the CI runner.
set -euo pipefail

cd "$(dirname "$0")/.."

# Step 1 — install the project + locked deps (orjson, tqdm) into the venv, so
# the frozen binary contains exactly what CI tests against.
uv sync --frozen

# Step 2 — build a single-file binary. --with adds PyInstaller for this run only
# (kept out of uv.lock). --collect-submodules tqdm pulls in tqdm.contrib.logging,
# which the CLI imports lazily and PyInstaller's static analysis can miss.
uv run --with pyinstaller pyinstaller \
  --onefile \
  --name subgraph \
  --collect-submodules tqdm \
  src/subgraph/__main__.py

# Step 3 — smoke test: confirm the bundle actually launches. PyInstaller appends
# .exe on Windows and leaves the name bare elsewhere.
if [ -f dist/subgraph.exe ]; then
  binary=dist/subgraph.exe
else
  binary=dist/subgraph
fi
"$binary" --help

echo
echo "Built $binary"
