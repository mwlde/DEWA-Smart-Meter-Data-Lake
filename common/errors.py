"""
Loud failure handling shared by the Spark scripts.

Every script announces each step as it starts ("[step] ..."), and if anything
goes wrong prints a banner naming the step, the error and what to do. The
banner goes to standard output, so it still shows when the command ends in
2>/dev/null (which hides Spark's own log output). Printed once only: spark-submit
merges a Python script's standard error into its standard output, so printing it
to both showed it twice. Spark's Java errors still go to standard error: rerun
without 2>/dev/null to see them.
"""

import sys
import traceback
from contextlib import contextmanager

_current = {"step": "starting up", "shown": False}


def banner(step, message, hint=None):
    """Print a failure banner without exiting (use inside streaming callbacks)."""
    lines = ["", "!" * 72, f"FAILED at step: {step}", f"Error: {message}"]
    if hint:
        lines.append(f"What to do: {hint}")
    lines += ["Full details: run the same command again without 2>/dev/null", "!" * 72, ""]
    text = "\n".join(lines)
    _current["shown"] = True
    print(text, flush=True)


def fail(step, message, hint=None):
    """Print a failure banner and stop the script."""
    banner(step, message, hint)
    sys.exit(1)


@contextmanager
def step(name):
    """Mark the start of a step, so a failure inside it is reported by name."""
    _current["step"] = name
    print(f"[step] {name}", flush=True)
    yield


def run(main):
    """Run main(); turn any uncaught error into a banner naming the current step."""
    try:
        main()
    except SystemExit:
        raise
    except KeyboardInterrupt:
        print("\nStopped by Ctrl+C.", flush=True)
        sys.exit(130)
    except Exception as e:
        traceback.print_exc()
        if _current["shown"]:
            print("\nStopped because of the error above.", flush=True)
            sys.exit(1)
        text = str(e).strip()
        first = next((ln for ln in text.splitlines() if ln.strip()), repr(e))
        fail(_current["step"], f"{type(e).__name__}: {first[:500]}")
