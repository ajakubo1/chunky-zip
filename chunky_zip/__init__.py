from __future__ import annotations

import os
from zipfile import ZIP_STORED

from chunky_zip.compressed_zip import ChunkedCompressedZipWriter
from chunky_zip.storage_zip import ChunkedStoredZipWriter


DEFAULT_CHUNK_SIZE = 1024 * 1024


# Helper functions remain the same as before
def read_file_in_chunks(file_obj, chunk_size=DEFAULT_CHUNK_SIZE):
    while True:
        chunk = file_obj.read(chunk_size)
        if not chunk:
            break
        yield chunk


def zip_file_in_chunks(input_file, output_zip, compression):
    filename_in_zip = os.path.basename(input_file)

    if compression == ZIP_STORED:
        with open(input_file, "rb") as src:
            for chunk in read_file_in_chunks(src):
                with ChunkedStoredZipWriter(output_zip, filename_in_zip) as zw:
                    zw.write_chunk(chunk)

            with ChunkedStoredZipWriter(output_zip, filename_in_zip) as zw:
                zw.flush()

    else:
        with open(input_file, "rb") as src:
            for chunk in read_file_in_chunks(src):
                with ChunkedCompressedZipWriter(
                    output_zip, filename_in_zip, compression
                ) as zw:
                    zw.write_chunk(chunk)

            with ChunkedCompressedZipWriter(
                output_zip, filename_in_zip, compression
            ) as zw:
                zw.flush()
