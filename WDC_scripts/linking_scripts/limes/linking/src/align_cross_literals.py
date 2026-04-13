#!/usr/bin/env python3
import argparse
import os
import sqlite3
import subprocess
import sys
import tempfile
import time
from typing import Iterator, Optional, Set, Tuple

NUMERIC_DT = {
    "http://www.w3.org/2001/XMLSchema#integer",
    "http://www.w3.org/2001/XMLSchema#int",
    "http://www.w3.org/2001/XMLSchema#long",
    "http://www.w3.org/2001/XMLSchema#short",
    "http://www.w3.org/2001/XMLSchema#byte",
    "http://www.w3.org/2001/XMLSchema#nonNegativeInteger",
    "http://www.w3.org/2001/XMLSchema#positiveInteger",
    "http://www.w3.org/2001/XMLSchema#nonPositiveInteger",
    "http://www.w3.org/2001/XMLSchema#negativeInteger",
    "http://www.w3.org/2001/XMLSchema#unsignedLong",
    "http://www.w3.org/2001/XMLSchema#unsignedInt",
    "http://www.w3.org/2001/XMLSchema#unsignedShort",
    "http://www.w3.org/2001/XMLSchema#unsignedByte",
    "http://www.w3.org/2001/XMLSchema#decimal",
    "http://www.w3.org/2001/XMLSchema#float",
    "http://www.w3.org/2001/XMLSchema#double",
}


def iter_nt_files(path: str) -> Iterator[str]:
    if os.path.isfile(path):
        yield path
        return

    entries = []
    for entry in os.scandir(path):
        if entry.is_file() and entry.name.endswith(".nt"):
            entries.append(entry.path)
    for item in sorted(entries):
        yield item


def parse_uri(token: str) -> Optional[str]:
    token = token.strip()
    if token.startswith("<") and token.endswith(">"):
        return token[1:-1]
    return None


def parse_nt_literal(token: str, lowercase: bool = False) -> Optional[Tuple[str, Optional[str]]]:
    t = token.strip()
    if not t.startswith('"'):
        return None

    i = 1
    escaped = False
    while i < len(t):
        c = t[i]
        if escaped:
            escaped = False
        else:
            if c == "\\":
                escaped = True
            elif c == '"':
                lex = t[1:i]
                rest = t[i + 1 :].strip()
                dtype = None
                if rest.startswith("^^<") and rest.endswith(">"):
                    dtype = rest[3:-1]
                try:
                    lex = bytes(lex, "utf-8").decode("unicode_escape")
                except Exception:
                    pass
                if lowercase:
                    lex = lex.lower()
                return lex, dtype
        i += 1
    return None


def parse_nt_line(line: str, lowercase: bool = False) -> Optional[Tuple[str, str]]:
    line = line.strip()
    if not line or line[0] == "#":
        return None
    if not line.endswith("."):
        return None
    line = line[:-1].strip()

    if not line.startswith("<"):
        return None
    s_end = line.find("> ")
    if s_end == -1:
        return None
    s = parse_uri(line[: s_end + 1])
    rest = line[s_end + 2 :].lstrip()

    if not rest.startswith("<"):
        return None
    p_end = rest.find("> ")
    if p_end == -1:
        return None
    o_raw = rest[p_end + 2 :].strip()

    if not s or not o_raw or not o_raw.startswith('"'):
        return None

    parsed = parse_nt_literal(o_raw, lowercase=lowercase)
    if not parsed:
        return None
    literal, dtype = parsed
    if dtype in NUMERIC_DT:
        return None
    return s, literal


def setup_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=OFF;")
    cur.execute("PRAGMA temp_store=MEMORY;")
    cur.execute("PRAGMA cache_size=-200000;")
    cur.execute(
        "CREATE TABLE IF NOT EXISTS left_literals (literal TEXT NOT NULL, uri TEXT NOT NULL, PRIMARY KEY (literal, uri));"
    )
    cur.execute(
        "CREATE TABLE IF NOT EXISTS right_literals (literal TEXT NOT NULL, uri TEXT NOT NULL, PRIMARY KEY (literal, uri));"
    )
    cur.execute("CREATE INDEX IF NOT EXISTS idx_left_literal ON left_literals(literal);")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_right_literal ON right_literals(literal);")
    conn.commit()
    return conn


def fmt_gb(nbytes: int) -> str:
    return f"{nbytes / (1024**3):.2f} GB"


def ingest(path: str, conn: sqlite3.Connection, table: str, lowercase: bool, commit_every: int = 200000, progress_every_s: int = 5, literal_filter: Optional[Set[str]] = None) -> int:
    cur = conn.cursor()
    inserted = 0
    seen = 0
    files = list(iter_nt_files(path))
    total_bytes = sum(os.path.getsize(p) for p in files)
    read_bytes = 0
    t0 = time.time()
    last_report = t0

    for nt_path in files:
        print(f"Ingesting {nt_path}", file=sys.stderr)
        with open(nt_path, "rb") as f:
            for raw in f:
                read_bytes += len(raw)
                try:
                    line = raw.decode("utf-8", "replace")
                except Exception:
                    continue
                parsed = parse_nt_line(line, lowercase=lowercase)
                if not parsed:
                    continue
                uri, literal = parsed
                if literal_filter is not None and literal not in literal_filter:
                    continue
                seen += 1
                cur.execute(f"INSERT OR IGNORE INTO {table}(literal, uri) VALUES (?, ?)", (literal, uri))
                inserted += cur.rowcount
                if seen % commit_every == 0:
                    conn.commit()
                now = time.time()
                if now - last_report >= progress_every_s:
                    elapsed = now - t0
                    pct = (read_bytes / total_bytes * 100.0) if total_bytes else 0.0
                    mb_s = (read_bytes / (1024**2)) / elapsed if elapsed > 0 else 0.0
                    print(
                        f"Progress {table}: {pct:6.2f}% | {fmt_gb(read_bytes)} / {fmt_gb(total_bytes)} | {mb_s:7.1f} MB/s | kept {inserted:,} unique subject-literal pairs",
                        file=sys.stderr,
                    )
                    last_report = now
    conn.commit()
    print(f"Finished {table}: {inserted:,} unique subject-literal pairs", file=sys.stderr)
    return inserted


def sort_unique(path_in: str, path_out: str, parallel: int = 8, mem: str = "50%") -> None:
    env = dict(os.environ)
    env["LC_ALL"] = "C"
    cmd = ["sort", "-S", mem, f"--parallel={parallel}", "-u", path_in]
    with open(path_out, "w", encoding="utf-8") as fout:
        subprocess.run(cmd, stdout=fout, check=True, env=env)


def emit_cross_pairs(conn: sqlite3.Connection, out_path: str, audit_path: Optional[str], parallel: int, sort_mem: str) -> Tuple[int, int]:
    cur = conn.cursor()
    cur.execute(
        "SELECT l.literal FROM left_literals l WHERE EXISTS (SELECT 1 FROM right_literals r WHERE r.literal = l.literal) GROUP BY l.literal ORDER BY l.literal"
    )
    overlap_literals = [row[0] for row in cur.fetchall()]
    print(f"Overlapping literals: {len(overlap_literals):,}", file=sys.stderr)

    out_dir = os.path.dirname(os.path.abspath(out_path)) or os.getcwd()
    os.makedirs(out_dir, exist_ok=True)

    with tempfile.TemporaryDirectory(prefix="cross_literal_", dir=out_dir) as tmpdir:
        pairs_tmp = os.path.join(tmpdir, "pairs.tmp.tsv")
        with open(pairs_tmp, "w", encoding="utf-8") as pair_out:
            audit_out = open(audit_path, "w", encoding="utf-8") if audit_path else None
            try:
                for literal in overlap_literals:
                    cur.execute("SELECT uri FROM left_literals WHERE literal = ? ORDER BY uri", (literal,))
                    left_uris = [row[0] for row in cur.fetchall()]
                    cur.execute("SELECT uri FROM right_literals WHERE literal = ? ORDER BY uri", (literal,))
                    right_uris = [row[0] for row in cur.fetchall()]
                    for left_uri in left_uris:
                        left_tok = f"<{left_uri}>"
                        for right_uri in right_uris:
                            right_tok = f"<{right_uri}>"
                            pair_out.write(f"{left_tok}\t{right_tok}\t1.0\n")
                            if audit_out is not None:
                                audit_out.write(f"{left_tok}\t{right_tok}\t1.0\t{literal}\n")
            finally:
                if audit_out is not None:
                    audit_out.close()

        sort_unique(pairs_tmp, out_path, parallel=parallel, mem=sort_mem)

    unique_pairs = 0
    with open(out_path, "r", encoding="utf-8") as f:
        for unique_pairs, _ in enumerate(f, start=1):
            pass

    return len(overlap_literals), unique_pairs


def main() -> None:
    ap = argparse.ArgumentParser(description="Cross-align two RDF N-Triples sources by shared non-numeric literal object values.")
    ap.add_argument("--left", required=True, help="Left input .nt file or directory of .nt files")
    ap.add_argument("--right", required=True, help="Right input .nt file or directory of .nt files")
    ap.add_argument("--db", required=True, help="SQLite working DB path")
    ap.add_argument("--out", required=True, help="Output TSV: <uri1>\\t<uri2>\\t1.0")
    ap.add_argument("--audit-out", default=None, help="Optional audit TSV with fourth column = shared literal")
    ap.add_argument("--lowercase", action="store_true", help="Lowercase lexical forms before matching (align.py style)")
    ap.add_argument("--parallel", type=int, default=8, help="Parallelism passed to sort")
    ap.add_argument("--sort-mem", default="50%", help="Memory budget passed to sort -S")
    ap.add_argument("--commit-every", type=int, default=200000)
    ap.add_argument("--progress-every-s", type=int, default=5)
    args = ap.parse_args()

    conn = setup_db(args.db)
    try:
        ingest(args.left, conn, "left_literals", lowercase=args.lowercase, commit_every=args.commit_every, progress_every_s=args.progress_every_s)
        left_literal_filter = {row[0] for row in conn.execute("SELECT literal FROM left_literals")}
        print(f"Filtering right side to {len(left_literal_filter):,} literals seen in the left dataset", file=sys.stderr)
        ingest(args.right, conn, "right_literals", lowercase=args.lowercase, commit_every=args.commit_every, progress_every_s=args.progress_every_s, literal_filter=left_literal_filter)
        overlap_literals, unique_pairs = emit_cross_pairs(conn, args.out, args.audit_out, parallel=args.parallel, sort_mem=args.sort_mem)
        print(f"Wrote {unique_pairs:,} unique cross-dataset pairs from {overlap_literals:,} overlapping literals to {args.out}", file=sys.stderr)
        if args.audit_out:
            print(f"Audit file written to {args.audit_out}", file=sys.stderr)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
