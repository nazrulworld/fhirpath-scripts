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

from helpers import FHIR_RELEASES
from helpers import cmd


__author__ = "Md Nazrul Islam (email2nazrul@gmail.com)"

BASE_URL = "http://hl7.org/fhir"
EXT = ".zip"
FHIR_DEFINITION = "definitions.json" + EXT
FHIR_EXAMPLE = "examples-json" + EXT
FHIR_SCHEMAS = "fhir.schema.json" + EXT

CACHE_DIR = pathlib.Path(__file__).parent / ".cache"


async def write_stream(filename, response):
    """ """
    # Progress Bar added
    with tqdm.tqdm(total=int(response.content_length)) as pbar:

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


async def download_version_info(session, release, offline):
    """ """
    uri = "/".join([BASE_URL, release, "version.info"])
    version_file = CACHE_DIR / release / "version.info"
    if offline and version_file.exists():
        return version_file

    try:
        async with await session.get(uri, allow_redirects=True) as response:
            if response.status == 200:
                with open(str(version_file), "w") as fp:
                    fp.write(await response.text())
        return version_file

    except (
        ServerDisconnectedError,
        ClientResponseError,
        ClientOSError,
        asyncio.TimeoutError,
    ) as exc:
        print(str(exc))
        sys.stderr.write(str(exc))


async def download_archieves(uris, session, offline):
    """ """
    global CACHE_DIR
    files = list()

    def find_release(uri):
        """ """
        for member in list(FHIR_RELEASES):
            if member.value in uri:
                return member.value

    release = find_release(uris[0])

    if not (CACHE_DIR / release).exists():
        (CACHE_DIR / release).mkdir()

    for uri in uris:
        cached_file = CACHE_DIR / release / uri.split("/")[-1]
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
async def main(destination_dir, output_stream, offline, verbosity_level, releases):
    """ """
    # try:
    #     response = requests.get(RESOURCE_URL)
    # except requests.RequestException as e:
    #     raise e
    global BASE_URL
    global FHIR_DEFINITION
    global FHIR_EXAMPLE
    global FHIR_SCHEMAS

    async with aiohttp.ClientSession() as session:

        for release in releases:

            v_info = await download_version_info(session, release, offline)
            sys.stdout.write(f"download version info on demand. {v_info}\n")
            continue
            archived_files = (
                "/".join([BASE_URL, release, FHIR_DEFINITION]),
                "/".join([BASE_URL, release, FHIR_EXAMPLE]),
                "/".join([BASE_URL, release, FHIR_SCHEMAS]),
            )
            results = await download_archieves(archived_files, session, offline)

            sys.stdout.write(f"{results} have been downloaded\n")
