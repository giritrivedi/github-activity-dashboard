# GitHub Activity Dashboard

A self-hosted, zero-dependency web dashboard that shows how much a set of GitHub users have contributed to a set of repositories. It counts **Pull Requests opened**, **Issues opened** and **Pull Requests reviewed**, by month or by quarter, and lets you drill down from any number to the underlying GitHub records.

- **Backend:** Python standard library only (`http.server`, `urllib`, `concurrent.futures`). No `pip install`.
- **Frontend:** a single-page app (HTML5, CSS3, vanilla JS) served from the same script. Charts are drawn on a native `<canvas>`, so no CDN or internet access is needed to render the UI.
- **Storage:** two local JSON files, `dashboard_config.json` (your users, repos and years) and `query_cache_v3.json` (cached query results).

---

## Table of contents

1. [Features](#features)
2. [Requirements](#requirements)
3. [Quick start](#quick-start)
4. [Usage](#usage)
5. [GitHub token (recommended)](#github-token-recommended)
6. [How it works](#how-it-works)
7. [Configuration and files](#configuration-and-files)
8. [HTTP API](#http-api)
9. [Error handling](#error-handling)
10. [Troubleshooting](#troubleshooting)
11. [Limitations](#limitations)
12. [Security notes](#security-notes)

---

## Features

- Select multiple users, repositories and years, using dual-list pickers (Available / Selected).
- Add users and repositories on the fly. They are validated against GitHub before being saved.
- Three views:
  - **Visual Dashboard:** KPI cards, stacked-bar trend chart, distribution doughnut, contributor comparison.
  - **Matrix Table:** user x metric x period, with grand totals.
  - **Contributor Cards:** per-user summary.
- **Monthly / Quarterly** toggle. Quarters are derived from the monthly data in the browser, so switching costs no extra GitHub queries.
- **Drill-down:** click any number to list the matching PRs, issues or reviewed PRs, with links to GitHub.
- **Export CSV** and **Print / PDF**.
- **Live progress** via Server-Sent Events (SSE).
- **Smart caching:** completed months are cached for 30 days and the current month for 6 hours. The cache is repo-set aware (see [How it works](#how-it-works)).
- **Rate-limit aware:** global throttling, retries with back-off, and honouring of `Retry-After` / `X-RateLimit-Reset`.

## Requirements

| Item | Requirement |
|------|-------------|
| Python | 3.8 or newer (developed and checked on 3.9) |
| OS | macOS, Linux or Windows |
| Network | Outbound HTTPS to `api.github.com` (and `github.com` when running without a token) |
| Browser | Any modern browser |
| Dependencies | None |

## Quick start

```bash
git clone https://github.com/<your-username>/github-activity-dashboard.git
cd github-activity-dashboard

# Optional but strongly recommended (see "GitHub token" below)
export GITHUB_TOKEN=ghp_xxxxxxxxxxxxxxxxxxxx

python3 github_activity_dashboard.py
```

Then open <http://localhost:8080>.

The first run creates `dashboard_config.json` in the **current working directory** with sample defaults. Run the script from the same folder each time so the config and cache are reused.

## Usage

1. **Pick users.** The left list shows saved users. Highlight one or more and click *Add to Selected Box* (or double-click). To add a new user, type a username and click *+ Create & Save*. It is checked against GitHub first.
2. **Pick repositories.** Same flow, using `owner/repo` format (for example `dotnet/runtime`).
3. **Pick years.** Add any year (for example `2023`). Future months are skipped automatically, and the current month is marked with `*` (till date).
4. **(Optional) Enter a token** in the token field. It is used for this session only and is never written to disk by the app.
5. Click **Generate Contributions Matrix**. A progress bar shows `done / total` queries.
6. Switch between **Visual Dashboard**, **Matrix Table** and **Contributor Cards**, and between **Monthly** and **Quarterly**.
7. Click any number in the Matrix or Cards view to open the detail list with links to GitHub.
8. Use **Export CSV**, **Print / PDF**, or **Clear Cache** (forces a full re-fetch on the next run) from the top-right.

The **Delete from Database** buttons permanently remove the highlighted items from `dashboard_config.json`.

### What each metric means

| Metric | GitHub search used | Notes |
|--------|-------------------|-------|
| Pull Requests | `type:pr author:<user>` | PRs created in the period |
| Issues | `type:issue author:<user>` | Issues created in the period |
| Code Reviews | `type:pr reviewed-by:<user>` | Counts **PRs reviewed**, not individual review events. The `created:` date filter applies to the PR creation date, not the review date. |

### Changing the port

Edit `PORT = 8080` at the top of the script.

## GitHub token (recommended)

| | With token | Without token |
|---|---|---|
| Data source | GitHub Search REST API | Scraping `github.com/search` HTML |
| Throttle | about 1 request per 2.2 s (under the 30/min search limit) | about 1 request per 3 s |
| Reliability | High | Fragile. GitHub may demand a login or rate-limit aggressively. |
| Drill-down details | Works | Works, but is limited to 60 req/h by the unauthenticated API |

Provide the token either way:

- Environment variable: `export GITHUB_TOKEN=...`
- Or paste it in the **GitHub Token** field in the UI. It is sent only to your local server, then forwarded to GitHub.

A classic token with no scopes, or a fine-grained token with public read access, is enough for public repositories. Private repositories need `repo` read access.

## How it works

```
Browser (SPA)  --POST /api/query-->  Python server  -->  GitHub Search API
      ^                                   |  ThreadPool (5 workers) + global throttle
      |<------ SSE progress / done -------|  repo-set-aware cache (query_cache_v3.json)
```

1. For each *(user, month, metric)* the server builds one search query, for example `type:pr author:octocat repo:a/b repo:c/d created:2025-03-01..2025-03-31`.
2. Tasks run on a pool of `MAX_WORKERS = 5` threads. A global lock spaces requests so the rate limit is respected no matter how many threads run.
3. **Repo-set-aware cache:** each cached entry remembers which repos it covered. A new request for repos `{A,B,C}` is answered by summing disjoint cached entries that are subsets of `{A,B,C}`. Only the uncovered repos are fetched, as a single combined query. This is exact because a PR or issue belongs to exactly one repo.
4. Results stream to the browser as SSE `progress` events, followed by one `done` event carrying the full matrix.
5. The browser aggregates months into quarters locally.

## Configuration and files

| File | Purpose |
|------|---------|
| `github_activity_dashboard.py` | The entire application |
| `dashboard_config.json` | Auto-created. Stores `users`, `repositories`, `years`. Safe to edit by hand while the server is stopped. |
| `query_cache_v3.json` | Auto-created. Cached counts. Safe to delete (or use **Clear Cache**). v1/v2 cache files are ignored. |

Tunable constants at the top of the script:

| Constant | Default | Meaning |
|----------|---------|---------|
| `PORT` | `8080` | HTTP port |
| `CACHE_TTL` | 6 h | Lifetime of current-month results |
| `CACHE_TTL_FINAL` | 30 days | Lifetime of completed-month results |
| `MAX_WORKERS` | `5` | Parallel GitHub requests |

## HTTP API

| Method | Path | Body | Description |
|--------|------|------|-------------|
| GET | `/` | | The web UI |
| GET | `/api/config` | | Current users / repos / years |
| GET | `/api/cache/clear` | | Delete the query cache |
| POST | `/api/config/user` | `{user, token?}` | Validate and add a user |
| POST | `/api/config/user/delete` | `{users: []}` | Remove users |
| POST | `/api/config/repo` | `{repo, token?}` | Validate and add a repo |
| POST | `/api/config/repo/delete` | `{repos: []}` | Remove repos |
| POST | `/api/config/year` | `{year}` | Add a year |
| POST | `/api/config/year/delete` | `{years: []}` | Remove years |
| POST | `/api/query` | `{users, repos, years, token?}` | Run the analysis. Returns an SSE stream. |
| POST | `/api/details` | `{user, metric, repos, years, period?, token?}` | List up to 100 matching records |

## Error handling

The app distinguishes a **genuine zero** from a **failed query**. Failures are never cached and never silently counted as real data.

| Situation | Behaviour |
|-----------|-----------|
| HTTP 403 / 429 / 5xx from GitHub | Retries up to 5 times with increasing back-off. Honours `Retry-After`, and waits for `X-RateLimit-Reset` when `X-RateLimit-Remaining` is 0. Waits are capped at 65 s. |
| HTTP 401 (bad token) or 422 (invalid query) | Fails immediately. Retrying cannot help. |
| Network error or timeout | Retries up to 5 times with linear back-off, then gives up with a `FetchError`. |
| No token and the HTML result count cannot be parsed | Raises a clear error suggesting you use a token. |
| A single query fails during a run | The run continues. Failed items appear as `0` in the table, and a pop-up lists the first 5 errors. Re-run after a minute. Successful results are already cached, so only the failures are retried. |
| Adding a non-existent user or repo | Rejected with a 400 and a readable message (`GitHub user '@x' does not exist.`). |
| Invalid username or repo format | Rejected by regex validation before any network call. |
| Malformed JSON in a request | 400 `Invalid JSON`. |
| Invalid or unsupported `period` / `metric` in drill-down | 400 with a message. |
| Corrupt or missing `dashboard_config.json` | Falls back to the built-in defaults. |
| Corrupt or unreadable cache file | Starts with an empty cache. |
| Cache file cannot be written | Ignored. The app keeps working with the in-memory cache. |
| Port already in use | Prints instructions (`lsof -i :8080`, `kill -9 <PID>`) and exits cleanly. |
| `Ctrl+C` | Stops the server gracefully. |

## Troubleshooting

| Symptom | Likely cause and fix |
|---------|----------------------|
| `Port 8080 is already in use` | Another instance is running. Run `lsof -i :8080` and `kill <PID>`, or change `PORT`. |
| Pop-up: "N queries failed and are shown as 0" | Rate limiting or a bad or missing token. Wait about 1 minute, set a valid token, and re-run. |
| `could not parse result count ... login may be required` | The tokenless web-scraping path was blocked. Set `GITHUB_TOKEN`. |
| `HTTP 401` | The token is invalid, expired or revoked. |
| `HTTP 422` | GitHub rejected the query, for example a repo you cannot access. Check repo names. |
| `HTTP 403` on adding a repo or user | Unauthenticated API limit (60/h) reached. Provide a token. |
| Numbers look stale | Click **Clear Cache** and re-run. Current-month data is cached for 6 hours. |
| Review counts do not match GitHub's profile page | By design: this counts PRs reviewed, filtered by PR creation date. |
| Config is not found or resets | You launched the script from a different directory. Config and cache live in the current working directory. |

## Limitations

- The GitHub Search API returns at most 1,000 results per query. This does not affect counts (`total_count`), but the drill-down lists are capped at 100 records.
- The search API is limited to 30 requests per minute (authenticated). Large selections (many users x many months x 3 metrics) take a while on the first run. Later runs are served from the cache.
- Review counts are PRs reviewed, not review events.
- The tokenless mode depends on GitHub's HTML and may break without notice.

## Security notes

- The server binds to all interfaces (`""`) on port 8080 and has **no authentication**. On untrusted networks, change the bind address in `run_server()` to `"127.0.0.1"`.
- Tokens entered in the UI are held in browser memory and sent only to your local server. They are not persisted. Prefer the `GITHUB_TOKEN` environment variable.
- Never commit `dashboard_config.json`, `query_cache_v3.json` or any token. The provided `.gitignore` excludes them.
