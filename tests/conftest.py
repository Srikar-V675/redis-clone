import pytest
import subprocess
import time
import socket

@pytest.fixture(scope="session")
def server():
    # Start the server
    proc = subprocess.Popen(
        ["python", "-m", "app.main"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )

    # Wait for the server to be ready
    timeout = 5
    for _ in range(timeout * 10):
        try:
            sock = socket.create_connection(("localhost", 6379), timeout=0.5)
            sock.close()
            break
        except OSError:
            time.sleep(0.1)
    else:
        proc.terminate()
        raise RuntimeError("Server didn't start in time")

    yield  # <- This is where the test actually runs

    # Teardown after all tests
    proc.terminate()
    proc.wait()