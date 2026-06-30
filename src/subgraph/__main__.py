"""Module entry point: enables ``python -m subgraph`` and is the target that
PyInstaller freezes into the standalone ``subgraph.exe`` (see the Windows
release workflow). Uses absolute imports so freezing it as the top-level
``__main__`` script does not pull the sibling modules in under bare names.
"""

import multiprocessing

from subgraph.cli import main

if __name__ == "__main__":
    # tqdm imports multiprocessing for its write lock, so a PyInstaller --onefile
    # build re-executes this exe to host the resource-tracker child. Without this
    # call that child would fall through to main() and argparse the internal args.
    # No-op when not frozen.
    multiprocessing.freeze_support()
    main()
