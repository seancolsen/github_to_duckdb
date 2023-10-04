#!/usr/bin/env python3

import argparse
import os
import re
import requests
import tempfile
import time
import tarfile

import duckdb
from dotenv import load_dotenv

load_dotenv()
token = os.getenv("GITHUB_PERSONAL_ACCESS_TOKEN")

description = "Export data from a GitHub repository to a DuckDB database."
parser = argparse.ArgumentParser(description=description)
parser.add_argument("repository", help='The user and repository name, e.g. "user/repo"')
parser.add_argument("db_file", help='The path to the database file, e.g. "repo.duckdb"')
parser.add_argument(
    "--resume",
    help="Resume an already-started migration using its numerical ID",
    type=int,
    metavar="ID",
    dest="resumed_migration_id",
)
parser.add_argument(
    "-a",
    "--archive-dir",
    help="Path to a directory where the archive data will be saved. If omitted, a temporary directory will be used",
    type=str,
    metavar="DIR",
    dest="archive_dir",
)
args = parser.parse_args()
org, repo = args.repository.split("/")

headers = {
    "Accept": "application/vnd.github+json",
    "Authorization": f"Bearer {token}",
    "X-GitHub-Api-Version": "2022-11-28",
}


def get_migration_id():
    if args.resumed_migration_id:
        return args.resumed_migration_id

    print("Starting migration.")
    # https://docs.github.com/en/rest/migrations/orgs?apiVersion=2022-11-28#start-an-organization-migration
    response = requests.post(
        url=f"https://api.github.com/orgs/{org}/migrations",
        headers=headers,
        json={
            "repositories": [f"{org}/{repo}"],
            "exclude_git_data": True,
            "exclude_attachments": True,
            "exclude_releases": True,
        },
    )
    response.raise_for_status()
    print(f"Migration started with id {migration_id}.")
    return response.json()["id"]


migration_id = get_migration_id()

archive_url = None
while True:
    url = f"https://api.github.com/orgs/{org}/migrations/{migration_id}"
    response = requests.get(url=url, headers=headers)
    response.raise_for_status()
    status = response.json()
    state = status["state"]
    print(f"Migration is {state}.")
    if state == "exported":
        archive_url = status["archive_url"]
        break
    elif state == "failed":
        print("Export failed.")
        exit(1)
    time.sleep(5)


def download(archive_dir):
    print("Downloading archive.")
    response = requests.get(url=archive_url, headers=headers, stream=True)
    response.raise_for_status()
    with tarfile.open(fileobj=response.raw, mode="r:gz") as tar:
        tar.extractall(path=archive_dir)
    files = os.listdir(archive_dir)
    entities = set(re.match(r"((?!_\d)[a-z_])+", f)[0] for f in files)
    con = duckdb.connect(args.db_file)
    for entity in entities:
        print(f"Importing {entity}.")
        sql = f"""
            CREATE TABLE {entity} AS
            select * from read_json_auto(
                'archive/{entity}*.json',
                union_by_name=true
            );
        """
        con.sql(sql)
    con.close()


if args.archive_dir:
    download(args.archive_dir)
else:
    with tempfile.TemporaryDirectory() as archive_dir:
        download(archive_dir)
