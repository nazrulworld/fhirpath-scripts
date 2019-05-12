# -*- coding: utf-8 -*-
# @Date    : 2018-05-28 18:30:52
# @Author  : Md Nazrul Islam (email2nazrul@gmail.com)
# @Link    : http://nazrul.me/
"""Downloader for fhir definition, examples, schema"""
# @Version : $Id$
# All imports here
import os
import io
import json
import pathlib
import subprocess
import tempfile
import zipfile
import shutil

from helpers import FHIR_RELEASES
from helpers import cmd


__author__ = "Md Nazrul Islam (email2nazrul@gmail.com)"

BASE_URL = "http://hl7.org/fhir"
EXT = ".zip"
FHIR_EXAMPLE = "examples-json" + EXT

CACHE_DIR = pathlib.Path(__file__).parent / ".cache"


async def download_archieves(release):
    """ """
    global CACHE_DIR
    res = subprocess.check_output(
        [str(CACHE_DIR.parent / "cli.py"), "download_fhir_resources", "-R", release]
    )
    return res


async def reduce_fhir_json(destination_dir, output_stream, release):
    """ """
    archive_file = CACHE_DIR / release / FHIR_EXAMPLE
    version_info = CACHE_DIR / release / "version.info"
    tmp_dir = tempfile.mkdtemp()
    dest_dir = pathlib.Path(destination_dir) / release
    if not dest_dir.exists():
        dest_dir.mkdir()

    shutil.copyfile(str(version_info), str(dest_dir / "version.info"))

    with zipfile.ZipFile(str(archive_file), "r") as zip_ref:
        zip_ref.extractall(tmp_dir)

    for filename in ["profiles-types.json", "profiles-resources.json"]:
        new_bundle = dict(entry=list())

        with io.open(os.path.join(tmp_dir, filename), "r", encoding="utf-8") as fp:
            bundle_json = json.loads(fp.read())

        for entry in bundle_json["entry"]:
            resource = entry["resource"].copy()
            if "StructureDefinition" == resource["resourceType"]:
                del resource["text"]
                del resource["snapshot"]
                new_bundle["entry"].append(
                    {"fullUrl": entry.get("fullUrl"), "resource": resource}
                )
        del bundle_json["entry"]
        new_bundle.update(bundle_json)
        newfilename = filename.split(".")[:-1] + ["min", "json"]
        with open(str(dest_dir / ".".join(newfilename)), "w", encoding="utf-8") as fp:
            json.dump(new_bundle, fp, indent=2)

    # Work with valuset
    with io.open(os.path.join(tmp_dir, "valuesets.json"), "r", encoding="utf-8") as fp:
        new_bundle = dict(entry=list())
        bundle_json = json.loads(fp.read())
        for entry in bundle_json["entry"]:
            resource = entry["resource"].copy()
            if "ValueSet" == resource["resourceType"]:
                assert "url" in resource

            elif "CodeSystem" == resource["resourceType"]:
                assert "url" in resource
                if "content" not in resource and "concept" not in resource:
                    continue
            else:
                continue

            del resource["text"]
            new_bundle["entry"].append(
                {"fullUrl": entry.get("fullUrl"), "resource": resource}
            )
        del bundle_json["entry"]
        new_bundle.update(bundle_json)
        with open(str(dest_dir / "valuesets.min.json"), "w", encoding="utf-8") as fp:
            json.dump(new_bundle, fp)

    shutil.rmtree(tmp_dir)


@cmd
async def main(destination_dir, output_stream, verbosity_level, releases):
    """ """
    global BASE_URL
    global FHIR_EXAMPLE

    for release in releases:
        rel = FHIR_RELEASES[release]
        archive_file = CACHE_DIR / rel.value / FHIR_EXAMPLE

        if not archive_file.exists():
            await download_archieves(rel.value)

        await reduce_fhir_json(destination_dir, output_stream, rel.value)
