#!/usr/bin/env python3
import argparse
import os
import re
import sqlite3
import sys
import time
from typing import Optional, Set, Tuple

RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"

DEFAULT_LABEL_PREDICATE = "http://www.w3.org/2000/01/rdf-schema#label"
DEFAULT_TYPE_URI = "http://schema.org/Observation"


def validate_sql_identifier(name: str) -> str:
    """
    Keeps SQLite table names safe because we use them in SQL strings.
    """
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
        raise ValueError(
            f"Invalid table name: {name!r}. Use only letters, numbers, and underscores; "
            "it must not start with a number."
        )
    return name


def parse_uri(token: str) -> Optional[str]:
    token = token.strip()
    if token.startswith("<") and token.endswith(">"):
        return token[1:-1]
    return None


def parse_nt_literal(token: str) -> Optional[str]:
    """
    Parses an N-Triples literal token and returns a normalized string key.

    Normalization:
      - drops language tags:  "x"@en  -> "x"
      - lowercases lexical form: "Hello" -> "hello"
      - keeps datatype:       "1"^^<dt> stays "1"^^<dt>

    Returns None if token is not a literal.
    """

    def lowercase_lex(lex: str) -> str:
        # Lowercase only literal characters; keep N-Triples escapes intact.
        if len(lex) < 2 or lex[0] != '"' or lex[-1] != '"':
            return lex

        out = ['"']
        i = 1
        end = len(lex) - 1

        while i < end:
            c = lex[i]

            if c == "\\" and i + 1 < end:
                esc = lex[i + 1]
                out.append(c)
                out.append(esc)
                i += 2

                if esc == "u":
                    for _ in range(4):
                        if i < end:
                            out.append(lex[i])
                            i += 1
                elif esc == "U":
                    for _ in range(8):
                        if i < end:
                            out.append(lex[i])
                            i += 1
                continue

            out.append(c.lower())
            i += 1

        out.append('"')
        return "".join(out)

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
                lex = t[: i + 1]
                rest = t[i + 1 :].strip()

                # Drop language tags, keep datatype.
                if rest.startswith("@"):
                    rest = ""

                return lowercase_lex(lex) + rest

        i += 1

    return None


def parse_nt_line(line: str) -> Optional[Tuple[str, str, str, bool]]:
    """
    Lightweight N-Triples parser for lines like:

      <s> <p> <o> .
      <s> <p> "literal"@en .
      <s> <p> "literal"^^<datatype> .

    Returns:
      (subject_uri, predicate_uri, object_value, object_is_literal)

    URI objects are returned without <...>.
    Literal objects are returned as normalized literal keys.
    """
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

    p = parse_uri(rest[: p_end + 1])
    o_raw = rest[p_end + 2 :].strip()

    if not s or not p or not o_raw:
        return None

    if o_raw.startswith("<"):
        o = parse_uri(o_raw)
        if o is None:
            return None
        return s, p, o, False

    lit = parse_nt_literal(o_raw)
    if lit is None:
        return None

    return s, p, lit, True


def setup_db(db_path: str, table_prefix: str) -> sqlite3.Connection:
    table_prefix = validate_sql_identifier(table_prefix)

    entities_table = f"{table_prefix}_entities"
    labels_table = f"{table_prefix}_labels"

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=OFF;")
    cur.execute("PRAGMA temp_store=MEMORY;")
    cur.execute("PRAGMA cache_size=-200000;")

    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {entities_table} (
            uri TEXT PRIMARY KEY
        );
        """
    )

    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {labels_table} (
            uri       TEXT NOT NULL,
            label     TEXT NOT NULL,
            predicate TEXT NOT NULL,
            PRIMARY KEY (uri, label, predicate)
        );
        """
    )

    cur.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{labels_table}_label ON {labels_table}(label);"
    )
    cur.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{labels_table}_uri ON {labels_table}(uri);"
    )

    conn.commit()
    return conn


def fmt_gb(nbytes: int) -> str:
    return f"{nbytes / (1024**3):.2f} GB"


def br(u: str) -> str:
    """
    Wrap raw URI as an N-Triples-style token.
    """
    return f"<{u}>"


def ingest(
    nt_path: str,
    conn: sqlite3.Connection,
    table_prefix: str,
    type_uris: Set[str],
    label_predicates: Set[str],
    commit_every: int = 200000,
    progress_every_s: int = 5,
) -> None:
    table_prefix = validate_sql_identifier(table_prefix)

    entities_table = f"{table_prefix}_entities"
    labels_table = f"{table_prefix}_labels"

    total_bytes = os.path.getsize(nt_path)
    read_bytes = 0

    cur = conn.cursor()

    n_type_hits = 0
    n_label_hits = 0
    n_relevant = 0

    t0 = time.time()
    last_report = t0

    print("Type URIs:", file=sys.stderr)
    for uri in sorted(type_uris):
        print(f"  {uri}", file=sys.stderr)

    print("Label predicates:", file=sys.stderr)
    for pred in sorted(label_predicates):
        print(f"  {pred}", file=sys.stderr)

    with open(nt_path, "rb") as f:
        for raw in f:
            read_bytes += len(raw)

            try:
                line = raw.decode("utf-8", "replace")
            except Exception:
                continue

            parsed = parse_nt_line(line)
            if not parsed:
                continue

            s, p, o, object_is_literal = parsed

            if p == RDF_TYPE and not object_is_literal and o in type_uris:
                cur.execute(
                    f"INSERT OR IGNORE INTO {entities_table}(uri) VALUES (?)",
                    (s,),
                )
                n_type_hits += 1
                n_relevant += 1

            elif p in label_predicates and object_is_literal:
                # Insert every label for this resource, but only if the resource has selected type.
                #
                # This works best when rdf:type appears before labels.
                # If labels appear before rdf:type in your file, use --two-pass.
                cur.execute(
                    f"""
                    INSERT OR IGNORE INTO {labels_table}(uri, label, predicate)
                    SELECT ?, ?, ?
                    WHERE EXISTS (
                        SELECT 1 FROM {entities_table}
                        WHERE uri = ?
                    )
                    """,
                    (s, o, p, s),
                )

                if cur.rowcount > 0:
                    n_label_hits += 1
                    n_relevant += 1

            if n_relevant > 0 and n_relevant % commit_every == 0:
                conn.commit()

            now = time.time()
            if now - last_report >= progress_every_s:
                elapsed = now - t0
                pct = (read_bytes / total_bytes * 100.0) if total_bytes else 0.0
                mb_s = (read_bytes / (1024**2)) / elapsed if elapsed > 0 else 0.0
                remaining_bytes = max(total_bytes - read_bytes, 0)
                eta_s = (remaining_bytes / (mb_s * 1024**2)) if mb_s > 0 else 0.0

                print(
                    f"Progress: {pct:6.2f}% | {fmt_gb(read_bytes)} / {fmt_gb(total_bytes)}"
                    f" | {mb_s:7.1f} MB/s | ETA ~ {eta_s/60:,.1f} min"
                    f" | type hits {n_type_hits:,}"
                    f" | labels stored {n_label_hits:,}",
                    file=sys.stderr,
                )

                last_report = now

    conn.commit()

    total_s = time.time() - t0
    print(
        f"Ingest done: read {fmt_gb(read_bytes)} in {total_s/60:.1f} min"
        f" | type hits {n_type_hits:,}"
        f" | labels stored {n_label_hits:,}",
        file=sys.stderr,
    )


def ingest_two_pass(
    nt_path: str,
    conn: sqlite3.Connection,
    table_prefix: str,
    type_uris: Set[str],
    label_predicates: Set[str],
    commit_every: int = 200000,
    progress_every_s: int = 5,
) -> None:
    """
    Safer version for files where labels may appear before rdf:type.

    Pass 1:
      collect resources with selected rdf:type

    Pass 2:
      collect all labels for those resources

    This scans the .nt file twice, but avoids missing labels that appear before type triples.
    """
    table_prefix = validate_sql_identifier(table_prefix)

    entities_table = f"{table_prefix}_entities"
    labels_table = f"{table_prefix}_labels"

    total_bytes = os.path.getsize(nt_path)
    cur = conn.cursor()

    print("Two-pass mode enabled.", file=sys.stderr)

    print("Pass 1/2: collecting typed resources...", file=sys.stderr)
    read_bytes = 0
    n_type_hits = 0
    t0 = time.time()
    last_report = t0

    with open(nt_path, "rb") as f:
        for raw in f:
            read_bytes += len(raw)

            try:
                line = raw.decode("utf-8", "replace")
            except Exception:
                continue

            parsed = parse_nt_line(line)
            if not parsed:
                continue

            s, p, o, object_is_literal = parsed

            if p == RDF_TYPE and not object_is_literal and o in type_uris:
                cur.execute(
                    f"INSERT OR IGNORE INTO {entities_table}(uri) VALUES (?)",
                    (s,),
                )
                n_type_hits += 1

                if n_type_hits % commit_every == 0:
                    conn.commit()

            now = time.time()
            if now - last_report >= progress_every_s:
                elapsed = now - t0
                pct = (read_bytes / total_bytes * 100.0) if total_bytes else 0.0
                mb_s = (read_bytes / (1024**2)) / elapsed if elapsed > 0 else 0.0
                remaining_bytes = max(total_bytes - read_bytes, 0)
                eta_s = (remaining_bytes / (mb_s * 1024**2)) if mb_s > 0 else 0.0

                print(
                    f"Pass 1: {pct:6.2f}% | {fmt_gb(read_bytes)} / {fmt_gb(total_bytes)}"
                    f" | {mb_s:7.1f} MB/s | ETA ~ {eta_s/60:,.1f} min"
                    f" | type hits {n_type_hits:,}",
                    file=sys.stderr,
                )

                last_report = now

    conn.commit()
    print(f"Pass 1 done: type hits {n_type_hits:,}", file=sys.stderr)

    print("Pass 2/2: collecting labels for typed resources...", file=sys.stderr)
    read_bytes = 0
    n_label_hits = 0
    t1 = time.time()
    last_report = t1

    with open(nt_path, "rb") as f:
        for raw in f:
            read_bytes += len(raw)

            try:
                line = raw.decode("utf-8", "replace")
            except Exception:
                continue

            parsed = parse_nt_line(line)
            if not parsed:
                continue

            s, p, o, object_is_literal = parsed

            if p in label_predicates and object_is_literal:
                cur.execute(
                    f"""
                    INSERT OR IGNORE INTO {labels_table}(uri, label, predicate)
                    SELECT ?, ?, ?
                    WHERE EXISTS (
                        SELECT 1 FROM {entities_table}
                        WHERE uri = ?
                    )
                    """,
                    (s, o, p, s),
                )

                if cur.rowcount > 0:
                    n_label_hits += 1

                    if n_label_hits % commit_every == 0:
                        conn.commit()

            now = time.time()
            if now - last_report >= progress_every_s:
                elapsed = now - t1
                pct = (read_bytes / total_bytes * 100.0) if total_bytes else 0.0
                mb_s = (read_bytes / (1024**2)) / elapsed if elapsed > 0 else 0.0
                remaining_bytes = max(total_bytes - read_bytes, 0)
                eta_s = (remaining_bytes / (mb_s * 1024**2)) if mb_s > 0 else 0.0

                print(
                    f"Pass 2: {pct:6.2f}% | {fmt_gb(read_bytes)} / {fmt_gb(total_bytes)}"
                    f" | {mb_s:7.1f} MB/s | ETA ~ {eta_s/60:,.1f} min"
                    f" | labels stored {n_label_hits:,}",
                    file=sys.stderr,
                )

                last_report = now

    conn.commit()

    total_s = time.time() - t0
    print(
        f"Two-pass ingest done in {total_s/60:.1f} min"
        f" | type hits {n_type_hits:,}"
        f" | labels stored {n_label_hits:,}",
        file=sys.stderr,
    )


def emit_pairs(
    conn: sqlite3.Connection,
    table_prefix: str,
    out_path: str,
    mode: str = "canonical",
    include_label_in_output: bool = False,
) -> None:
    """
    mode:
      - canonical:
          For each duplicate label group, pick smallest URI as canonical and pair it with others.
      - allpairs:
          Emit all unique unordered pairs in the group.

    Default output TSV:
      <uri1>\t<uri2>\t1.0

    With --include-label-in-output:
      <uri1>\t<uri2>\t1.0\t"normalized label"
    """
    table_prefix = validate_sql_identifier(table_prefix)
    labels_table = f"{table_prefix}_labels"

    cur = conn.cursor()

    cur.execute(
        f"""
        SELECT label
        FROM {labels_table}
        GROUP BY label
        HAVING COUNT(DISTINCT uri) > 1
        """
    )

    dup_labels = [row[0] for row in cur.fetchall()]
    print(f"Duplicate labels found: {len(dup_labels):,}", file=sys.stderr)

    n_pairs = 0

    with open(out_path, "w", encoding="utf-8") as out:
        for label in dup_labels:
            cur.execute(
                f"""
                SELECT DISTINCT uri
                FROM {labels_table}
                WHERE label = ?
                ORDER BY uri
                """,
                (label,),
            )

            uris = [row[0] for row in cur.fetchall()]

            if len(uris) < 2:
                continue

            if mode == "allpairs":
                for i in range(len(uris)):
                    for j in range(i + 1, len(uris)):
                        if include_label_in_output:
                            out.write(f"{br(uris[i])}\t{br(uris[j])}\t1.0\t{label}\n")
                        else:
                            out.write(f"{br(uris[i])}\t{br(uris[j])}\t1.0\n")
                        n_pairs += 1
            else:
                canon = uris[0]
                for u in uris[1:]:
                    if include_label_in_output:
                        out.write(f"{br(canon)}\t{br(u)}\t1.0\t{label}\n")
                    else:
                        out.write(f"{br(canon)}\t{br(u)}\t1.0\n")
                    n_pairs += 1

    print(f"Pairs written: {n_pairs:,}", file=sys.stderr)


def emit_label_inventory(
    conn: sqlite3.Connection,
    table_prefix: str,
    out_path: str,
) -> None:
    """
    Optional debug/inventory output:

      label<TAB>number_of_unique_resources<TAB>number_of_label_triples
    """
    table_prefix = validate_sql_identifier(table_prefix)
    labels_table = f"{table_prefix}_labels"

    cur = conn.cursor()

    with open(out_path, "w", encoding="utf-8") as out:
        cur.execute(
            f"""
            SELECT label, COUNT(DISTINCT uri) AS resource_count, COUNT(*) AS triple_count
            FROM {labels_table}
            GROUP BY label
            ORDER BY resource_count DESC, triple_count DESC, label ASC
            """
        )

        for label, resource_count, triple_count in cur:
            out.write(f"{label}\t{resource_count}\t{triple_count}\n")


def main() -> None:
    ap = argparse.ArgumentParser(
        description=(
            "Generate duplicate-label link pairs for selected rdf:type resources from an N-Triples file. "
            "Supports multiple label predicates and multiple labels per resource."
        )
    )

    ap.add_argument("--input", required=True, help="Path to .nt file.")
    ap.add_argument("--db", default="labels.sqlite", help="SQLite DB path.")
    ap.add_argument("--out", required=True, help="Output TSV: <uri1>\\t<uri2>\\t1.0")

    ap.add_argument(
        "--type-uri",
        action="append",
        dest="type_uris",
        help=(
            "RDF type URI to match. Repeat this option to include multiple types. "
            f'Example: --type-uri "{DEFAULT_TYPE_URI}" '
            '--type-uri "http://schema.org/Place". '
            f"If not provided, defaults to {DEFAULT_TYPE_URI}"
        ),
    )

    ap.add_argument(
        "--label-predicate",
        action="append",
        dest="label_predicates",
        help=(
            "Predicate URI used as a label. Repeat this option to include multiple predicates. "
            f'Example: --label-predicate "{DEFAULT_LABEL_PREDICATE}" '
            '--label-predicate "http://schema.org/name". '
            f"If not provided, defaults to {DEFAULT_LABEL_PREDICATE}"
        ),
    )

    ap.add_argument(
        "--table",
        default="entities",
        help=(
            "SQLite table prefix. The script creates <prefix>_entities and <prefix>_labels. "
            "Use only letters, numbers, and underscores."
        ),
    )

    ap.add_argument(
        "--mode",
        choices=["canonical", "allpairs"],
        default="canonical",
        help=(
            "canonical = one canonical URI linked to every other URI with same label. "
            "allpairs = all pair combinations per label."
        ),
    )

    ap.add_argument(
        "--two-pass",
        action="store_true",
        help=(
            "Scan input twice. Use this if labels may appear before rdf:type triples. "
            "Safer, but slower."
        ),
    )

    ap.add_argument(
        "--include-label-in-output",
        action="store_true",
        help="Add the duplicate label as a fourth TSV column.",
    )

    ap.add_argument(
        "--label-inventory",
        default=None,
        help="Optional TSV output: label, unique resource count, label triple count.",
    )

    ap.add_argument("--commit-every", type=int, default=200000)
    ap.add_argument("--progress-every-s", type=int, default=5)

    args = ap.parse_args()

    validate_sql_identifier(args.table)

    if not os.path.isfile(args.input):
        raise FileNotFoundError(f"Input file does not exist: {args.input}")

    output_dir = os.path.dirname(os.path.abspath(args.out)) or os.getcwd()
    os.makedirs(output_dir, exist_ok=True)

    if args.label_inventory:
        inventory_dir = os.path.dirname(os.path.abspath(args.label_inventory)) or os.getcwd()
        os.makedirs(inventory_dir, exist_ok=True)

    type_uris = set(args.type_uris) if args.type_uris else {DEFAULT_TYPE_URI}

    label_predicates = (
        set(args.label_predicates)
        if args.label_predicates
        else {DEFAULT_LABEL_PREDICATE}
    )

    conn = setup_db(args.db, args.table)

    if args.two_pass:
        ingest_two_pass(
            args.input,
            conn,
            table_prefix=args.table,
            type_uris=type_uris,
            label_predicates=label_predicates,
            commit_every=args.commit_every,
            progress_every_s=args.progress_every_s,
        )
    else:
        ingest(
            args.input,
            conn,
            table_prefix=args.table,
            type_uris=type_uris,
            label_predicates=label_predicates,
            commit_every=args.commit_every,
            progress_every_s=args.progress_every_s,
        )

    if args.label_inventory:
        emit_label_inventory(conn, args.table, args.label_inventory)

    emit_pairs(
        conn,
        table_prefix=args.table,
        out_path=args.out,
        mode=args.mode,
        include_label_in_output=args.include_label_in_output,
    )

    conn.close()


if __name__ == "__main__":
    main()