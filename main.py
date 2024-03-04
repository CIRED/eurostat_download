# Copyright © 2024 CIRED Nicolas Graves <ngraves@ngraves.fr>
# SPDX-License-Identifier: GPL-3.0-or-later

import os
import requests
import gzip
import tempfile
from tqdm import tqdm

eurostat_data_url = "ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/"


def download_data(url, output_path, compressed=True):
    # Act as a cache: download only if it is necessary.
    if os.path.exists(output_path):
        print(output_path, " already exists.")
    else:
        # Use a temporary file for downloading
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            response = requests.get(url, stream=True)

            # Save the file with tqdm for progress tracking
            with tqdm.wrapattr(
                temp_file, "write", total=int(response.headers.get("content-length", 0))
            ) as t:
                for chunk in tqdm(response.iter_content(chunk_size=1024)):
                    if chunk:
                        t.write(chunk)

            # Extract contents based on file extension
            temp_file_path = temp_file.name
            if compressed:
                with gzip.open(temp_file_path, "rb") as gz_file:
                    content = gz_file.read()
            else:
                with open(temp_file_path, "r") as file:
                    content = file.read()

            # Save the extracted content to the final destination
            with open(output_path, "w") as output_file:
                output_file.write(content)

            # Remove the temporary file
            os.remove(temp_file_path)


def get_base_url():
    return eurostat_data_url


def get_url(table_name):
    return (
        f"https://{eurostat_data_url}{table_name.upper()}/?format=TSV&compressed=true"
    )
