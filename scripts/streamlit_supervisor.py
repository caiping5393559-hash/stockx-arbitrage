from __future__ import annotations

import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
DATA_DIR.mkdir(exist_ok=True)

supervisor_log = DATA_DIR / "streamlit_supervisor.log"
stdout_log = DATA_DIR / "streamlit_start_stdout.log"
stderr_log = DATA_DIR / "streamlit_start_stderr.log"


def write_log(message: str) -> None:
    timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
    with supervisor_log.open("a", encoding="utf-8") as handle:
        handle.write(f"{timestamp} {message}\n")


def main() -> None:
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
    while True:
        write_log("starting streamlit")
        with stdout_log.open("ab") as stdout, stderr_log.open("ab") as stderr:
            process = subprocess.Popen(
                args,
                cwd=str(BASE_DIR),
                stdin=subprocess.DEVNULL,
                stdout=stdout,
                stderr=stderr,
                close_fds=True,
            )
            write_log(f"streamlit pid={process.pid}")
            return_code = process.wait()
            write_log(f"streamlit exited code={return_code}")
        time.sleep(3)


if __name__ == "__main__":
    main()
