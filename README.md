# GitHub Export to DuckDB

Use this tool to export data from your GitHub repository into a [DuckDB](https://duckdb.org/) database. You'll end up with a single file which contains the following tables:

- `bots`
- `issue_comments`
- `issue_events`
- `issues`
- `milestones`
- `organizations`
- `pull_request_review_comments`
- `pull_request_review_threads`
- `pull_request_reviews`
- `pull_requests`
- `repositories`
- `schema`
- `users`

Then you can use SQL to perform analytics on that data.

## Installation

1. `pip install -r requirements.txt`
1. Copy `.env.example` to `.env`.
1. Generate a GitHub [personal access token](https://github.com/settings/tokens) and paste into `.env`.

## Usage

```
./github_to_duckdb.py organization/repository my_repo.db
```

## How it works

1. It uses the GitHub API to initiate a [migration](https://docs.github.com/en/rest/migrations/orgs?apiVersion=2022-11-28#about-organization-migrations) of your repository. This does not change any data within the repo.

1. It waits while GitHub generates the migration archive. This may take several minutes.

1. It downloads and unzips the migration data to a temporary directory on your machine.

1. It loads the migration data into a DuckDB database and saves that database file to the specified path.
