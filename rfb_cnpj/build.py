from glob import glob
from io import TextIOWrapper
from os import makedirs
from os.path import basename, getsize, isfile, join
from shutil import copyfileobj, move
from tempfile import NamedTemporaryFile
from urllib.parse import urljoin
from zipfile import ZipFile

from httpx import AsyncClient
from tqdm import tqdm

import asyncio


#
# Constants
#

MAX_CONCURRENT_DOWNLOADS = 8

# Deve terminar por /
ROOT_URL = "https://dados.rfb.gov.br/CNPJ/"

# Relativos a pyproject.toml
ZIP_SOURCES_FOLDER = "./data/0-zip_sources/"
CSV_SOURCES_FOLDER = "./data/1-csv_sources/"
PARQUET_SOURCES_FOLDER = "./data/2-parquet_sources/"

# Todos arquivos a baixar (exceto Socios) -- Ago/2024
SOURCE_FILES = [
    "dados_abertos_cnpj/2024-08/Cnaes.zip",
    "dados_abertos_cnpj/2024-08/Empresas0.zip",
    "dados_abertos_cnpj/2024-08/Empresas1.zip",
    "dados_abertos_cnpj/2024-08/Empresas2.zip",
    "dados_abertos_cnpj/2024-08/Empresas3.zip",
    "dados_abertos_cnpj/2024-08/Empresas4.zip",
    "dados_abertos_cnpj/2024-08/Empresas5.zip",
    "dados_abertos_cnpj/2024-08/Empresas6.zip",
    "dados_abertos_cnpj/2024-08/Empresas7.zip",
    "dados_abertos_cnpj/2024-08/Empresas8.zip",
    "dados_abertos_cnpj/2024-08/Empresas9.zip",
    "dados_abertos_cnpj/2024-08/Estabelecimentos0.zip",
    "dados_abertos_cnpj/2024-08/Estabelecimentos1.zip",
    "dados_abertos_cnpj/2024-08/Estabelecimentos2.zip",
    "dados_abertos_cnpj/2024-08/Estabelecimentos3.zip",
    "dados_abertos_cnpj/2024-08/Estabelecimentos4.zip",
    "dados_abertos_cnpj/2024-08/Estabelecimentos5.zip",
    "dados_abertos_cnpj/2024-08/Estabelecimentos6.zip",
    "dados_abertos_cnpj/2024-08/Estabelecimentos7.zip",
    "dados_abertos_cnpj/2024-08/Estabelecimentos8.zip",
    "dados_abertos_cnpj/2024-08/Estabelecimentos9.zip",
    "dados_abertos_cnpj/2024-08/Motivos.zip",
    "dados_abertos_cnpj/2024-08/Municipios.zip",
    "dados_abertos_cnpj/2024-08/Naturezas.zip",
    "dados_abertos_cnpj/2024-08/Paises.zip",
    "dados_abertos_cnpj/2024-08/Qualificacoes.zip",
    "dados_abertos_cnpj/2024-08/Simples.zip",
    "regime_tributario/Imunes e isentas.zip",
    "regime_tributario/Lucro Arbitrado.zip",
    "regime_tributario/Lucro Presumido 1.zip",
    "regime_tributario/Lucro Real.zip",
]


#
# Functions
#


# asyncio.gather limited to 'n' concurrent tasks -- https://stackoverflow.com/a/61478547/33244
async def gather_with_semaphore(n, *tasks):
    semaphore = asyncio.Semaphore(n)

    async def wrapped_task(task):
        async with semaphore:
            return await task

    return await asyncio.gather(*(wrapped_task(c) for c in tasks))


# Download multiple files concurrently
async def download_files_async(root_url, files, output_path):
    BAR_FORMAT = "{desc: <25} {percentage:3.0f}% {bar} [{remaining}, {rate_fmt}]"

    async def download_file_async(root_url, file_name, target_path, position):
        full_url = urljoin(root_url, file_name)
        full_target_file = join(target_path, basename(file_name))
        if isfile(full_target_file):
            # File already exists: Just update progress to 100%.
            total = getsize(full_target_file)
            with tqdm(
                desc=f"  {file_name}",
                total=total,
                leave=False,
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
                bar_format=BAR_FORMAT,
                position=position,
                dynamic_ncols=True,
            ) as progress:
                progress.update(total)
            return

        with NamedTemporaryFile(delete=False) as temp_file:
            async with AsyncClient() as client:
                async with client.stream("GET", full_url) as response:
                    total = int(response.headers["Content-Length"])

                    with tqdm(
                        desc=f"  {file_name}",
                        total=total,
                        leave=False,
                        unit="B",
                        unit_scale=True,
                        unit_divisor=1024,
                        bar_format=BAR_FORMAT,
                        position=position,
                        dynamic_ncols=True,
                    ) as progress:
                        num_bytes_downloaded = response.num_bytes_downloaded
                        async for chunk in response.aiter_bytes():
                            temp_file.write(chunk)
                            progress.update(
                                response.num_bytes_downloaded - num_bytes_downloaded
                            )
                            num_bytes_downloaded = response.num_bytes_downloaded

        move(temp_file.name, full_target_file)

    tasks = [
        download_file_async(root_url, file, output_path, i)
        for i, file in enumerate(files)
    ]
    await gather_with_semaphore(MAX_CONCURRENT_DOWNLOADS, *tasks)
    return len(files)


# Extract files from zip archive and convert them from 'latin1' to 'utf-8'
def extract_files(input_files, output_path):
    # ToDo: Async, parallel, tqdm.
    #   https://stackoverflow.com/questions/77078023/progress-bar-when-copying-one-large-file-in-python
    file_count = 0
    for zip_file in input_files:
        with ZipFile(zip_file, "r") as zip_ref:
            names = zip_ref.namelist()
            for n in names:
                member = zip_ref.getinfo(n)

                target_file = join(output_path, member.filename)
                if not isfile(target_file):
                    with zip_ref.open(member) as s:
                        with open(target_file, "w", encoding="utf-8") as t:
                            copyfileobj(TextIOWrapper(s, encoding="latin1"), t)

                print(f"    {target_file}")
                file_count += 1

    return file_count


#
# Main
#


async def main_async():
    # L0: Baixa os arquivos zip do site da RFB. (5.5 GB, ~10 min)
    print("L0: Downloading files...")
    makedirs(ZIP_SOURCES_FOLDER, exist_ok=True)
    file_count = await download_files_async(ROOT_URL, SOURCE_FILES, ZIP_SOURCES_FOLDER)
    print(f"\r        {file_count} zip files ready.")

    # L1: Extrai os arquivos csv e converte de 'latin1' para 'utf-8'. (20.4 GB, ~3 min)
    print("L1: Extracting files...")
    makedirs(CSV_SOURCES_FOLDER, exist_ok=True)
    all_zip_files = glob(join(ZIP_SOURCES_FOLDER, "*.zip"))
    file_count = extract_files(all_zip_files, CSV_SOURCES_FOLDER)
    print(f"        {file_count} csv files ready.")

    # L2: Cria pasta para os arquivos parquet.
    makedirs(PARQUET_SOURCES_FOLDER, exist_ok=True)


def main():
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
