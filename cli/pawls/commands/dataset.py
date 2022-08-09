import glob
import hashlib
import logging
import os
import shutil
import tempfile
from pathlib import Path
from typing import Union

import boto3
import click
from click import BadArgumentUsage
from tqdm import tqdm

s3_resource = boto3.resource(
    's3'
)


def hash_pdf(file: Union[str, Path]) -> str:
    block_size = 65536

    file_hash = hashlib.sha256()
    with open(str(file), 'rb') as fp:
        fb = fp.read(block_size)
        while len(fb) > 0:
            file_hash.update(fb)
            fb = fp.read(block_size)

    return str(file_hash.hexdigest())


def copy(source: Union[str, Path], destination: Union[str, Path]) -> None:
    shutil.copy(str(source), str(destination))


def move(source: Union[str, Path], destination: Union[str, Path]) -> None:
    shutil.move(str(source), str(destination))


@click.command(context_settings={"help_option_names": ["--help", "-h"]})
@click.argument("directory", type=click.Path(exists=True, file_okay=True, dir_okay=True))
@click.option("--no-hash", is_flag=True)
def add(directory: click.Path, no_hash: bool) -> None:
    """
    Add a PDF or directory of PDFs to the pawls dataset (skiff_files/).
    """
    base_dir = Path("skiff_files/apps/pawls/papers")
    base_dir.mkdir(exist_ok=True, parents=True)

    if os.path.isdir(str(directory)):
        pdfs = glob.glob(os.path.join(str(directory), "*.pdf"))
    else:
        pdfs = [str(directory)]

    logging.info(f"Found {len(pdfs)} total PDFs to add.")

    for pdf in tqdm(pdfs):
        pdf_name = Path(pdf).stem

        if not no_hash:
            pdf_name = hash_pdf(pdf)

        output_dir = base_dir / pdf_name

        if output_dir.exists() and no_hash:
            logging.warning(f"PDF with name {pdf_name}.pdf already added. Skipping...")
            continue
        elif output_dir.exists():
            logging.warning(f"{pdf} already added. Skipping...")
            continue

        output_dir.mkdir(exist_ok=True)

        copy(pdf, output_dir / (pdf_name + '.pdf'))


@click.command(context_settings={"help_option_names": ["--help", "-h"]})
@click.argument("bucket_name", type=str)
@click.argument("directory", type=str)
@click.option("--no-hash", is_flag=True)
def add_from_s3(bucket_name: str, directory: str, no_hash: bool) -> None:
    """
    Add a PDF or directory of PDFs to the pawls dataset (skiff_files/) from s3.
    :param bucket_name: s3 bucket name
    :param directory: PDF path or directory path
    :param no_hash: add hash to pdf file names
    """

    # Check bucket if exists
    bucket = s3_resource.Bucket(bucket_name)

    if not bucket.creation_date:
        raise BadArgumentUsage("Invalid bucket: bucket does not exist")

    if directory.startswith("/"):
        directory = directory[1:]

    files = [f for f in bucket.objects.filter(Prefix=directory) if f.key.endswith('.pdf')]

    # Check directory or path
    if not files:
        raise BadArgumentUsage("Invalid directory or path: directory or path does not exist")

    base_dir = Path("skiff_files/apps/pawls/papers")
    base_dir.mkdir(exist_ok=True, parents=True)

    logging.info(f"Found {len(files)} total PDFs to add.")

    file_equivalents = {}

    for s3_pdf in tqdm(files):
        # Download files with a temporary path
        with tempfile.NamedTemporaryFile(mode='w', suffix='.pdf', delete=False) as f:
            bucket.download_file(s3_pdf.key, f.name)
            pdf_name = Path(s3_pdf.key).stem
            if not no_hash:
                pdf_name = hash_pdf(f.name)

            file_equivalents[pdf_name] = f.name

    # Move the files to the corresponding path
    for file_stem, filepath in file_equivalents.items():
        output_dir = base_dir / file_stem

        pdf = f'{file_stem}.pdf'

        if output_dir.exists() and no_hash:
            logging.warning(f"PDF with name {pdf} already added. Skipping...")
            continue
        elif output_dir.exists():
            logging.warning(f"{file_stem} already added. Skipping...")
            continue

        output_dir.mkdir(exist_ok=True)

        move(filepath, output_dir / pdf)
