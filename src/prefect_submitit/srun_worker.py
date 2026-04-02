"""Worker entry point for srun steps.

Invoked as::

    python -m prefect_submitit.srun_worker <job_folder>

Reads ``job.pkl`` (cloudpickle-serialized callable), executes it, and writes
the result to ``result.pkl`` via atomic temp-file-then-rename.

Exit code protocol:
    0  — task completed (success or failure captured in the result envelope)
    >0 — bootstrap failure (could not load pickle, could not write result)
"""

from __future__ import annotations

import os
import pickle
import sys
import tempfile
import traceback
from pathlib import Path


def main(job_folder: str) -> None:
    """Load a pickled callable, execute it, and write the result envelope."""
    folder = Path(job_folder)
    job_path = folder / "job.pkl"
    result_path = folder / "result.pkl"

    with open(job_path, "rb") as f:
        fn = pickle.load(f)

    try:
        result = fn()
        envelope = {"status": "ok", "result": result}
    except Exception as exc:
        envelope = {
            "status": "error",
            "error": str(exc),
            "type": type(exc).__name__,
            "traceback": traceback.format_exc(),
        }

    # Atomic write: temp file then rename
    fd, tmp_path = tempfile.mkstemp(dir=folder, suffix=".tmp")
    with os.fdopen(fd, "wb") as f:
        pickle.dump(envelope, f)
    os.rename(tmp_path, str(result_path))


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(
            "Usage: python -m prefect_submitit.srun_worker <job_folder>",
            file=sys.stderr,
        )
        sys.exit(2)
    main(sys.argv[1])
