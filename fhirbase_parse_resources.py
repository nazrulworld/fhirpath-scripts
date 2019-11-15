# -*- coding: utf-8 -*-
# @Date    : 2018-05-28 18:30:52
# @Author  : Md Nazrul Islam (email2nazrul@gmail.com)
# @Link    : http://nazrul.me/
# @Version : $Id$
# All imports here
import io
import json
import os
import shutil
import tempfile
import zipfile

import download_fhirbase_resource
from helpers import cmd


__author__ = "Md Nazrul Islam (email2nazrul@gmail.com)"


async def build_sql(concepts_tables, func_file, schema_file):
    """ """
    output_fp = io.StringIO()
    with io.open(schema_file, "r") as fp:
        data = json.load(fp)
        for stmt in data:
            if not stmt:
                continue
            output_fp.write(stmt.strip() + "\n")

    with io.open(func_file, "r") as fp:
        data = json.load(fp)
        for stmt in data:
            if not stmt:
                continue
            output_fp.write(stmt + "\n")
    output_fp.write("\n")
    for stmt in concepts_tables:
        output_fp.write(stmt.strip() + "\n\n")
    return output_fp


async def build_json(concepts_tables, func_file, schema_file):

    output_fp = io.StringIO()
    container = list()
    with io.open(schema_file, "r") as fp:
        data = json.load(fp)
        for stmt in data:
            if not stmt:
                continue
            container.append(stmt.strip())

    with io.open(func_file, "r") as fp:
        data = json.load(fp)
        for stmt in data:
            if not stmt:
                continue
            container.append(stmt.strip())

    for stmt in concepts_tables:
        container.append(stmt.strip())

    json.dump(container, output_fp, indent=2)

    return output_fp


async def parse(archive_file, schema_version, callback):
    """ """
    tmp_dir = tempfile.mkdtemp()

    with zipfile.ZipFile(archive_file, "r") as zip_ref:
        zip_ref.extractall(tmp_dir)

    func_file, fhir_resource_file = None, None
    for root, dirs, files in os.walk(tmp_dir):
        if func_file is not None and fhir_resource_file is not None:
            break
        for filename in files:
            if not filename.endswith(".sql.json"):
                continue
            if filename == "functions.sql.json":
                func_file = os.path.join(root, filename)
            if filename == "fhirbase-{0}.sql.json".format(schema_version):
                fhir_resource_file = os.path.join(root, filename)

    concepts_tables = [
        """CREATE TABLE IF NOT EXISTS "concept" (
    id text primary key,
    txid bigint not null,
    ts timestamptz DEFAULT current_timestamp,
    resource_type text default 'Concept',
    status resource_status not null,
    resource jsonb not null
);""",
        """CREATE TABLE IF NOT EXISTS "concept_history" (
    id text,
    txid bigint not null,
    ts timestamptz DEFAULT current_timestamp,
    resource_type text default 'Concept',
    status resource_status not null,
    resource jsonb not null,
    PRIMARY KEY (id, txid)
);""",
    ]
    fpobj = await callback(concepts_tables, func_file, fhir_resource_file)
    shutil.rmtree(tmp_dir)

    return fpobj


@cmd
async def main(
    destination_dir, fhirbase_release, output_format, offline, verbosity_level
):
    """ """
    await download_fhirbase_resource.main(offline, verbosity_level)
    destination_dir = destination_dir or tempfile.mkdtemp()
    print(destination_dir)

    for rel in fhirbase_release:
        file_ = download_fhirbase_resource.FHIRBASE_DIR / "{0}.zip".format(rel)

        if output_format == "sql":
            callback = build_sql
            ext = ".sql"
        else:
            callback = build_json
            ext = ".json"

        fpobj = await parse(file_, "4.0.0", callback)

        with io.open(os.path.join(destination_dir, "fhirbase-4.0.0" + ext), "w") as fp:
            fp.write(fpobj.getvalue())

        fpobj.flush()
        fpobj.close()
