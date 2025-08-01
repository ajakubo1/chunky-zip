from __future__ import annotations

from datetime import datetime
from typing import Optional, List, IO
from zipfile import (
    ZipFile,
    ZipInfo,
    ZIP_STORED,
)


try:
    import zlib  # We may need its compression method

    Z_FULL_FLUSH = zlib.Z_FULL_FLUSH
    crc32 = zlib.crc32
except ImportError:
    import binascii

    Z_FULL_FLUSH = 3
    crc32 = binascii.crc32

ZIP64_LIMIT = (1 << 31) - 1


class ChunkedStoredZipWriter:
    """A class to write data in chunks to a single file within a ZIP archive without persistent temporary storage."""

    def __init__(self, output_zip: str, filename_in_zip: str):
        """
        Initialize the chunked ZIP writer.
        """
        self.output_zip = output_zip
        self.filename_in_zip = filename_in_zip
        self.compression = ZIP_STORED
        self.zipinfo = self._extract_zipinfo()

    def _extract_zipinfo(self) -> ZipInfo:
        try:
            with ZipFile(self.output_zip, "r", compression=self.compression) as zf:
                return zf.getinfo(self.filename_in_zip)
        except (KeyError, FileNotFoundError):
            # If entry doesn't exist, create new ZipInfo (temp remains empty)
            zipinfo = ZipInfo(self.filename_in_zip, datetime.now().timetuple()[:6])
            zipinfo.compress_type = self.compression
            zipinfo.CRC = crc32(b"")
            zipinfo.file_size = 0
            zipinfo.compress_size = 0
            zipinfo.header_offset = 0
            return zipinfo

    def _exclude_file_info(self, zip_infos: List[ZipInfo]):
        return list(filter(lambda z: z.filename != self.filename_in_zip, zip_infos))

    def swap_zipinfo(self, zip_file: ZipFile):
        zip_file.filelist = self._exclude_file_info(zip_file.filelist)
        zip_file.filelist.append(self.zipinfo)
        zip_file.NameToInfo[self.filename_in_zip] = self.zipinfo

    def update_zip_info_time(self):
        self.zipinfo.date_time = datetime.now().timetuple()[:6]

    def update_zipinfo_data(self, chunk: Optional[bytes], compressed_data: bytes):
        self.update_zip_info_time()
        self.zipinfo.compress_size += len(compressed_data)
        if chunk is not None:
            self.zipinfo.file_size += len(chunk)
            self.zipinfo.CRC = crc32(chunk, self.zipinfo.CRC)

    def force_zip64(self):
        # Logic taken from zipfile
        return self.zipinfo.file_size * 1.05 > ZIP64_LIMIT

    def write_zipinfo(self, write_stream: IO[bytes] | None):
        if write_stream is None:
            raise ValueError("Attempt to write to ZIP archive that was already closed")

        write_stream.seek(self.zipinfo.header_offset)
        write_stream.write(self.zipinfo.FileHeader(self.force_zip64()))

    def write_appended_data(self, write_stream: IO[bytes] | None, data: bytes):
        if write_stream is None:
            raise ValueError("Attempt to write to ZIP archive that was already closed")

        initial_offset = write_stream.tell()
        write_stream.seek(initial_offset + self.zipinfo.compress_size)
        write_stream.write(data)

    def update_start_dir(self, zip_file: ZipFile, write_stream: IO[bytes] | None):
        if write_stream is None:
            raise ValueError("Attempt to write to ZIP archive that was already closed")

        zip_file.start_dir = write_stream.tell()

    def _write(self, chunk: Optional[bytes], compressed_data: bytes) -> None:
        with ZipFile(self.output_zip, "a", compression=self.compression) as zf:
            self.update_zip_info_time()
            self.swap_zipinfo(zf)

            self.write_zipinfo(zf.fp)

            self.write_appended_data(zf.fp, compressed_data)
            self.update_zipinfo_data(chunk, compressed_data)

            self.update_start_dir(zf, zf.fp)

            self.write_zipinfo(zf.fp)

            zf._didModify = True

    def write_chunk(self, chunk: bytes):
        """
        Write a chunk of data to the ZIP file and update metadata.
        """
        self._write(chunk, chunk)

    def flush(self):
        """flush all of the leftover data in the ZIP compressor"""
        pass

    def close(self):
        """Finalize the ZIP file (optional, as each write_chunk updates it)."""
        pass  # No persistent state to clean up; ZIP is updated after each chunk

    def __enter__(self):
        """Support for context manager."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Ensure cleanup."""
        self.close()
