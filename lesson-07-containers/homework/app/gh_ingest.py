"""GitHub Archive Ingestor — завантажує годину подій у PostgreSQL.

Дано. Не редагувати.
"""
import csv, gzip, io, json, os, sys, urllib.request
from pathlib import Path
import psycopg

ARCHIVE_URL = os.environ.get("ARCHIVE_URL", "https://data.gharchive.org/2024-01-15-14.json.gz")
CACHE_DIR = Path(os.environ.get("CACHE_DIR", "/cache"))
PGHOST = os.environ["PGHOST"]
PGPORT = os.environ.get("PGPORT", "5432")
PGUSER = os.environ["PGUSER"]
PGPASSWORD = os.environ["PGPASSWORD"]
PGDATABASE = os.environ["PGDATABASE"]
TARGET_TYPES = frozenset(["PushEvent","PullRequestEvent","IssueCommentEvent","WatchEvent","IssuesEvent"])

def _download(url, dest):
    if dest.exists():
        print(f"Cache hit: {dest}")
        return dest
    dest.parent.mkdir(parents=True, exist_ok=True)
    print(f"Downloading {url} ...")
    urllib.request.urlretrieve(url, dest)
    print(f"Saved to {dest} ({dest.stat().st_size/1_000_000:.1f} MB)")
    return dest

def _parse_events(gz_path):
    seen = set()
    with gzip.open(gz_path, "rt", encoding="utf-8") as f:
        for line in f:
            ev = json.loads(line)
            etype, eid = ev.get("type"), ev.get("id")
            repo = (ev.get("repo") or {}).get("name", "")
            if etype not in TARGET_TYPES or not repo or eid in seen:
                continue
            seen.add(eid)
            yield {"event_id": eid, "event_type": etype,
                   "actor_login": (ev.get("actor") or {}).get("login", ""),
                   "repo_name": repo, "created_at": ev.get("created_at", "")}

def main():
    gz_path = _download(ARCHIVE_URL, CACHE_DIR / ARCHIVE_URL.rsplit("/",1)[-1])
    print("Parsing events ...")
    events = list(_parse_events(gz_path))
    print(f"Filtered to {len(events)} events across {len(set(e['event_type'] for e in events))} types")
    conninfo = f"host={PGHOST} port={PGPORT} user={PGUSER} password={PGPASSWORD} dbname={PGDATABASE}"
    print(f"Connecting to PostgreSQL ({PGHOST}:{PGPORT}/{PGDATABASE}) ...")
    with psycopg.connect(conninfo) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE TABLE IF NOT EXISTS github_events (event_id BIGINT PRIMARY KEY, event_type TEXT NOT NULL, actor_login TEXT NOT NULL, repo_name TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL);")
            cur.execute("TRUNCATE github_events;")
        conn.commit()
        buf = io.StringIO()
        writer = csv.writer(buf)
        for e in events:
            writer.writerow([e["event_id"], e["event_type"], e["actor_login"], e["repo_name"], e["created_at"]])
        buf.seek(0)
        with conn.cursor() as cur:
            with cur.copy("COPY github_events (event_id,event_type,actor_login,repo_name,created_at) FROM STDIN WITH CSV") as copy:
                copy.write(buf.read())
        conn.commit()
    print(f"Done. Loaded {len(events)} rows into github_events.")

if __name__ == "__main__":
    main()
