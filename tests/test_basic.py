import os
from zipfile import ZIP_BZIP2, ZIP_DEFLATED, ZIP_LZMA, ZIP_STORED

import pytest

from chunky_zip import DEFAULT_CHUNK_SIZE, zip_file_in_chunks
from tests.utils import generate_file, get_file_hash, get_zip_file_hash, memory_monitor

zipfile_setup = [
    [
        ("./test_file_1kb.txt", 1024, "random", compression_type),
        (
            "./test_file_1mb.txt",
            1024 * 1024,
            "random",
            compression_type,
        ),
        (
            "./test_file_10mb.txt",
            10 * 1024 * 1024,
            "random",
            compression_type,
        ),
        (
            "./test_file_10mb_repetitive.txt",
            10 * 1024 * 1024,
            "repetitive",
            compression_type,
        ),
        (
            "./test_file_15mb_repetitive.txt",
            15 * 1024 * 1024 + 1024 // 2,
            "repetitive",
            compression_type,
        ),
    ]
    for compression_type in [ZIP_DEFLATED, ZIP_STORED, ZIP_BZIP2, ZIP_LZMA]
]


@pytest.mark.parametrize(
    "file_name, file_size, data_type, compression",
    [val for row in zipfile_setup for val in row],
)
def test_zip(file_name, file_size, data_type, compression):
    output_zip = file_name.replace(".txt", f"_{compression}.zip")

    # Clean up
    try:
        os.remove(output_zip)
    except FileNotFoundError:
        pass

    generate_file(file_name, file_size, data_type)
    original_hash = get_file_hash(file_name)
    original_size = os.path.getsize(file_name)

    with memory_monitor(os.getpid()) as memory_samples:
        zip_file_in_chunks(file_name, output_zip, compression)

    zip_hash = get_zip_file_hash(output_zip, os.path.basename(file_name))

    assert original_hash == zip_hash

    zip_size = os.path.getsize(output_zip)

    max_memory = max(memory_samples) if memory_samples else 0
    # assert max_memory <  2 * DEFAULT_CHUNK_SIZE

    # Clean up
    os.remove(output_zip)