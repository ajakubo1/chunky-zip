from __future__ import annotations

from tempfile import NamedTemporaryFile
from typing import Dict, Optional, Protocol, Union
from zipfile import (
    ZIP_BZIP2,
    ZIP_DEFLATED,
    ZIP_LZMA,
    ZipFile,
    ZIP_STORED,
)

from chunky_zip.storage_zip import ChunkedStoredZipWriter

try:
    import zlib  # We may need its compression method

    Z_FULL_FLUSH = zlib.Z_FULL_FLUSH
except ImportError:
    Z_FULL_FLUSH = 3

ZIP64_LIMIT = (1 << 31) - 1


class EmptyCompress:
    def compress(self, data: bytes) -> bytes:
        return data

    def flush(self) -> bytes:
        return b""


class SupportsCompress(Protocol):
    def compress(self, to_compress: bytes) -> bytes: ...
    def flush(self, mode: Optional[int]) -> bytes: ...


class SupportsCompressEmptyFlush(Protocol):
    def compress(self, to_compress: bytes) -> bytes: ...
    def flush(self) -> bytes: ...


zip_compressor_storage: Dict[
    str, Union[SupportsCompress, SupportsCompressEmptyFlush]
] = {}


class ChunkedCompressedZipWriter(ChunkedStoredZipWriter):
    """A class to write data in chunks to a single file within a ZIP archive without persistent temporary storage."""

    def __init__(self, output_zip: str, filename_in_zip: str, compression=ZIP_DEFLATED):
        """
        Initialize the chunked ZIP writer.
        """
        self.output_zip = output_zip
        self.filename_in_zip = filename_in_zip
        self.compression = compression
        self.zipinfo = self._extract_zipinfo()

    @property
    def _compressor_path(self):
        return f"{self.output_zip}/{self.filename_in_zip}"

    @property
    def _compressor(self):
        return self.get_compressor()

    def get_compressor(self):
        if self.compression == ZIP_STORED:
            return EmptyCompress()
        elif self._compressor_path in zip_compressor_storage:
            return zip_compressor_storage[self._compressor_path]
        else:
            with (
                NamedTemporaryFile() as temp,
                ZipFile(temp, "w", compression=self.compression) as zip_file,
                zip_file.open(self.filename_in_zip, "w") as zip_writer,
            ):
                try:
                    compressor = zip_writer._compressor
                    zip_writer._compressor = None
                except AttributeError:
                    compressor = EmptyCompress()
                if compressor is None:
                    compressor = EmptyCompress()
                zip_compressor_storage[self._compressor_path] = compressor
            return compressor

    def write_chunk(self, chunk: bytes):
        """
        Write a chunk of data to the ZIP file and update metadata.
        """
        data = self._compressor.compress(chunk)

        self._write(chunk, data)

    def flush(self):
        if self.compression not in [ZIP_BZIP2, ZIP_LZMA]:
            return

        buffered_data = self._compressor.flush()

        if len(buffered_data) == 0:
            return

        self._write(None, buffered_data)

    def close(self):
        """Finalize the ZIP file"""
        if self.compression != ZIP_DEFLATED:
            return

        buffered_data = self._compressor.flush(Z_FULL_FLUSH)

        if len(buffered_data) == 0:
            return

        self._write(None, buffered_data)
