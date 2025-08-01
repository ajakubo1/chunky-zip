import hashlib
import os
import time
from contextlib import contextmanager
from typing import Any, Generator
from zipfile import ZipFile

import psutil  # type: ignore


def generate_file(filename, size_bytes, content_type="random"):
    chunk_size = 1024 * 1024  # 1MB chunks for writing

    if os.path.exists(filename):
        return

    with open(filename, "wb") as f:
        bytes_written = 0
        if content_type == "random":
            while bytes_written < size_bytes:
                chunk = os.urandom(min(chunk_size, size_bytes - bytes_written))
                f.write(chunk)
                bytes_written += len(chunk)
        elif content_type == "repetitive":
            text = b"Hello, World! " * 1000  # Highly compressible
            while bytes_written < size_bytes:
                chunk = text[: min(len(text), size_bytes - bytes_written)]
                f.write(chunk)
                bytes_written += len(chunk)


# Helper function to compute file hash
def get_file_hash(filename: str):
    """Compute SHA-256 hash of a file."""
    sha256 = hashlib.sha256()
    with open(filename, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


# Helper function to compute hash of file inside ZIP
def get_zip_file_hash(zip_filename, inner_filename):
    """Compute SHA-256 hash of a file inside a ZIP archive."""
    sha256 = hashlib.sha256()
    with ZipFile(zip_filename, "r") as zf:
        with zf.open(inner_filename) as f:
            for chunk in iter(lambda: f.read(65536), b""):
                sha256.update(chunk)
    return sha256.hexdigest()


# Context manager to monitor memory usage
@contextmanager
def memory_monitor(pid: int, interval=0.1) -> Generator[list, Any, None]:
    """
    Monitor memory usage of the current process.

    Args:
        pid (int): Process ID to monitor.
        interval (float): Polling interval in seconds.

    Yields:
        list: List of memory usage samples (RSS in bytes).
    """
    process = psutil.Process(pid)
    memory_samples = []

    base_rss = process.memory_info().rss

    def poll_memory():
        while True:
            try:
                mem_info = process.memory_info()
                memory_samples.append(mem_info.rss - base_rss)
                time.sleep(interval)
            except psutil.NoSuchProcess:
                break

    import threading

    t = threading.Thread(target=poll_memory, daemon=True)
    t.start()
    try:
        yield memory_samples
    finally:
        t.join(timeout=1.0)
