import subprocess
import sys


def test_stubtest():
    """Verify that the .pyi stub matches the runtime module."""
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "mypy.stubtest",
            "talsi._talsi",
            "--ignore-disjoint-bases",
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, f"stubtest failed:\n{result.stdout}\n{result.stderr}"
