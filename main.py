import os
import requests
import zipfile
import tempfile
from tqdm import tqdm


def download_and_process_data(url, output_path, compressed=True):
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
            base, extension = os.path.splitext(temp_file_path)
            if compressed:
                with zipfile.ZipFile(temp_file_path, "r") as zip_file:
                    content = zip_file.read(zip_file.namelist()[0])
            else:
                with open(temp_file_path, "r") as file:
                    content = file.read()

            # Save the extracted content to the final destination
            with open(output_path, "w") as output_file:
                output_file.write(content)

            # Remove the temporary file
            os.remove(temp_file_path)
