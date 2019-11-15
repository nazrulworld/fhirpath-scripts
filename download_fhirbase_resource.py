# -*- coding: utf-8 -*-
# @Date    : 2018-05-28 18:30:52
# @Author  : Md Nazrul Islam (email2nazrul@gmail.com)
# @Link    : http://nazrul.me/
"""Downloader for fhir definition, examples, schema"""
# @Version : $Id$
# All imports here
import asyncio
import io
import os
import pathlib
import sys

import aiohttp
import tqdm
from aiohttp.client_exceptions import ClientOSError
from aiohttp.client_exceptions import ClientResponseError
from aiohttp.client_exceptions import ServerDisconnectedError

from helpers import cmd


__author__ = "Md Nazrul Islam (email2nazrul@gmail.com)"

BASE_URL = "https://github.com/fhirbase/fhirbase/archive/{ver}.zip"
EXT = "zip"
VERSIONS = {"v0.0.6", "v0.0.5", "v0.0.4", "master"}

CACHE_DIR = pathlib.Path(__file__).parent / ".cache"
FHIRBASE_DIR = CACHE_DIR / "FHIRBASE"


async def write_stream(filename, response):
    """ """
    # Progress Bar added
    with tqdm.tqdm(total=int(response.content_length or 0)) as pbar:

        try:
            with open(filename, "wb") as f:
                while True:
                    chunk = await response.content.read(io.DEFAULT_BUFFER_SIZE)
                    if not chunk:
                        break
                    pbar.update(len(chunk))
                    f.write(chunk)
        except (
            ServerDisconnectedError,
            ClientResponseError,
            ClientOSError,
            asyncio.TimeoutError,
        ) as exc:
            print(str(exc))
            sys.stderr.write(str(exc))
            os.unlink(filename)


async def download_archieves(session, offline):
    """ """
    global CACHE_DIR
    files = list()

    if not FHIRBASE_DIR.exists():
        FHIRBASE_DIR.mkdir()

    for ver in VERSIONS:
        cached_file = FHIRBASE_DIR / (".".join([ver, EXT]))
        uri = BASE_URL.format(ver=ver)
        if offline:
            if not cached_file.exists():
                sys.stdout.write(
                    "No offline file found! at {0!s}, "
                    "going to dowanload fresh".format(cached_file)
                )
            else:
                files.append((uri, cached_file))
                continue

        try:
            async with await session.get(uri, allow_redirects=True) as response:
                if response.status == 200:
                    sys.stdout.write("Start downloading file from {0}\n".format(uri))
                    await write_stream(str(cached_file), response)
                    files.append((uri, cached_file))

        except (
            ServerDisconnectedError,
            ClientResponseError,
            ClientOSError,
            asyncio.TimeoutError,
        ) as exc:
            print(str(exc))
            sys.stderr.write(str(exc))
    return files


@cmd
async def main(offline, verbosity_level):
    """ """
    # try:
    #     response = requests.get(RESOURCE_URL)
    # except requests.RequestException as e:
    #     raise e

    async with aiohttp.ClientSession() as session:

        results = await download_archieves(session, offline)
        sys.stdout.write(f"{results} have been downloaded\n")
