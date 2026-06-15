from __future__ import annotations

import subprocess
import sys
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

stdout_log = DATA_DIR / "streamlit_start_stdout.log"
stderr_log = DATA_DIR / "streamlit_start_stderr.log"

creationflags = int(getattr(subprocess, "CREATE_NO_WINDOW", 0))

args = [
    sys.executable,
    "-m",
    "streamlit",
    "run",
    str(BASE_DIR / "app.py"),
    "--server.headless",
    "true",
    "--server.port",
    "8501",
    "--browser.gatherUsageStats",
    "false",
    "--server.folderWatchBlacklist",
    "data",
]

with stdout_log.open("ab") as stdout, stderr_log.open("ab") as stderr:
    process = subprocess.Popen(
        args,
        cwd=str(BASE_DIR),
        stdin=subprocess.DEVNULL,
        stdout=stdout,
        stderr=stderr,
        creationflags=creationflags,
        close_fds=True,
    )

print(process.pid)
