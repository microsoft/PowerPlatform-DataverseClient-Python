"""Wrapper that tees stdout/stderr to a log file while preserving interactive TTY."""
import sys
import os
import io


class _Tee:
    def __init__(self, original, log_file):
        self._original = original
        self._log = log_file

    def write(self, data):
        self._original.write(data)
        self._original.flush()
        self._log.write(data)
        self._log.flush()

    def flush(self):
        self._original.flush()
        self._log.flush()

    def __getattr__(self, name):
        return getattr(self._original, name)


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} <log_file> <script.py> [args...]")
        sys.exit(1)

    log_path = sys.argv[1]
    script = sys.argv[2]
    sys.argv = sys.argv[2:]  # make the target script see its own argv

    with open(log_path, "w", encoding="utf-8") as log_file:
        sys.stdout = _Tee(sys.__stdout__, log_file)
        sys.stderr = _Tee(sys.__stderr__, log_file)

        with open(script) as f:
            code = compile(f.read(), script, "exec")
            exec(code, {"__name__": "__main__", "__file__": script})
