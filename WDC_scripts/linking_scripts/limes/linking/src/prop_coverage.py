from __future__ import annotations

import re
import math
import hashlib
import logging
from dataclasses import dataclass
from helper import compute_cache_filename
from typing import Dict, List, Optional, Any, Union

class HyperLogLog:
    __slots__ = ("p", "m", "M")

    def __init__(self, p: int = 18):
        assert 4 <= p <= 20, "p outside safe range"
        self.p = p
        self.m = 1 << p
        self.M = bytearray(self.m)

    @staticmethod
    def _hash64(x: bytes) -> int:
        return int.from_bytes(hashlib.blake2b(x, digest_size=8).digest(), "big", signed=False)
    
    def add(self, value: Union[bytes, str]):
        if isinstance(value, str):
            value = value.encode("utf-8", "ignore")
        x = self._hash64(value)
        idx = x >> (64 - self.p)
        w = (x << self.p) & ((1 << 64) - 1)
        rho = 1
        if w != 0:
            rho = (w.bit_length() ^ 63) + 1
        maxbits = 64 - self.p
        if rho > maxbits:
            rho = maxbits
        if self.M[idx] < rho:
            self.M[idx] = rho

    def merge(self, other: "HyperLogLog"):
        assert self.p == other.p
        for i in range(self.m):
            if other.M[i] > self.M[i]:
                self.M[i] = other.M[i]

    def count(self) -> int:
        m = self.m
        if m == 16:
            alpha_m = 0.673
        elif m == 32:
            alpha_m = 0.697
        elif m == 64:
            alpha_m = 0.709
        else:
            alpha_m = 0.7213 / (1 + 1.079 / m)

        Z = sum(2.0 ** (-v) for v in self.M)
        E = alpha_m * (m * m) / Z

        V = self.M.count(0)
        if E <= 2.5 * m and V > 0:
            E = m * math.log(m / V)

        return int(E)
    

IRI   = r"<[^>]*>"
BNODE = r"_:[A-Za-z][A-Za-z0-9]*"
LIT   = r"\"(?:[^\"\\]|\\.)*\"(?:@[A-Za-z][A-Za-z0-9-]*|\^\^<[^>]*>)?"
TRIPLE = re.compile(
    rf"""^\s*
    (?P<s>{IRI}|{BNODE})\s+
    (?P<p>{IRI})\s+
    (?P<o>{IRI}|{BNODE}|{LIT})
    (?:\s+(?P<g>{IRI}|{BNODE}))?
    \s*\.\s*$
    """,
    re.X,
)


def is_literal(tok: str) -> bool:
    return tok.startswith('"')

def strip_brackets(iri:str) -> str:
    return iri[1:-1] if iri.startswith("<") and iri.endswith(">") else iri


@dataclass
class CoverageRow:
    p: str
    count: int
    coverage: float

def _subject_in_sample(s: str, sample: float) -> bool:
    if sample >= 1.0:
        return True
    h = HyperLogLog._hash64(s.encode("utf-8", "ignore"))
    v = (h >> 11) / float(1 << 53)
    return v < sample

def coverage_from_local(
        infile: str,
        hll_p: int = 18,
        sample: float = 1.0,
        top_k: int = 10,
        min_count: int = 0,
        class_iris: Optional[List[str]] = None,
) -> List[CoverageRow]:
    den_hll = HyperLogLog(p=hll_p)
    per_prop: Dict[str, HyperLogLog] = {}

    with open(infile, "rt", encoding="utf-8", errors="replace") as fin:
        for line in fin:
            m = TRIPLE.match(line)
            if not m:
                continue
            s, p, o = m.group("s"), m.group("p"), m.group("o")
            if not is_literal(o):
                continue

            if not _subject_in_sample(s, sample):
                continue

            sid = s
            den_hll.add(sid)
            if p not in per_prop:
                per_prop[p] = HyperLogLog(p=hll_p)
            per_prop[p].add(sid)

    denom = max(1, den_hll.count())
    rows: List[CoverageRow] = []
    for p, h in per_prop.items():
        c = h.count()
        if c < min_count:
            continue
        cov = (100.0 * c) / denom
        rows.append(CoverageRow(p=p, count=c, coverage=cov))

    rows.sort(key=lambda r: (r.coverage, r.count), reverse=True)
    return rows[:top_k] if top_k > 0 else rows

def to_jsonable(rows: List[CoverageRow]) -> List[dict]:
    return [{"property": r.p, "count": r.count, "coverage": r.coverage} for r in rows]

def _sleep_with_jitter(base: float, attempt: int, cap: float = 60.0):
    import random, time
    delay = min(base * (2 ** (attempt - 1)), cap)
    time.sleep(random.uniform(0, delay))

def _write_nt_line(s_iri: str, p_iri: str, lit: dict, out):
    v = lit["value"]
    v_esc = v.replace('\\', '\\\\').replace('"', '\\"')
    o = f"\"{v_esc}\""
    if "xml:lang" in lit:
        o += f"@{lit['xml:lang']}"
    elif "datatype" in lit:
        o += f"^^<{lit['datatype']}>"
    out.write(f"<{s_iri}> <{p_iri}> {o} .\n")

def _basic_triples_pattern(graph: str) -> str:
    inner = "?s ?p ?o .\n FILTER(isLiteral(?o))"
    if graph:
        return f"GRAPH <{graph}> {{ {inner} }}"
    return inner

def _sample_query_offset(offset: int, limit: int, graph: str) -> str:
    return f"""
SELECT ?s ?p ?o WHERE {{
  {_basic_triples_pattern(graph)}
}}
OFFSET {offset}
LIMIT {limit}
"""

def collect_sample_to_nt(endpoint: str,
                         out_path: str,
                         *,
                         total_pages: int = 6,
                         page_limit: int = 100,
                         runner_kwargs = None,
                         graph: str = "") -> int:
    
    if runner_kwargs is None:
        runner_kwargs = dict(max_retries=2, hard_timeout_ms=60000)

    gaps = [0]
    gap = page_limit

    for _ in range(total_pages - 1):
        gaps.append(gaps[-1] + gap)
        gap *= 2

    with open(out_path, 'wt', encoding='utf-8') as _:
        pass

    total = 0
    with open(out_path, 'at', encoding="utf-8") as out:
        logging.info(f'Saving triples to {out_path}')
        for off in gaps:
            q = _sample_query_offset(off, page_limit, graph)
            try:
                res = _run_sparql(endpoint, q, **runner_kwargs)
            except Exception:
                continue
            bindings = res.get("results", {}).get("bindings", [])
            if not bindings:
                continue
            for b in bindings:
                _write_nt_line(b["s"]["value"], b["p"]["value"], b["o"], out)
            total += len(bindings)
            if len(bindings) < page_limit:
                break

    return total

def _run_sparql(endpoint: str, query: str, *, max_retries: int = 6, base_backoff: float = 1.5, hard_timeout_ms: int = 120000) -> Dict[str, Any]:
    try:
        from SPARQLWrapper import SPARQLWrapper, JSON, POST
    except Exception as e:
        raise RuntimeError("SPARQLWrapper is required for SPARQL mode") from e
    
    from urllib.error import HTTPError, URLError
    import json
    
    UA = "WHALE/1.0 (contact: akhomich@mail.uni-paderborn.de)"
    attempt = 0
    last_err: Optional[Exception] = None

    while attempt < max_retries:
        attempt += 1
        try:
            sp = SPARQLWrapper(endpoint)
            sp.setReturnFormat(JSON)
            sp.setMethod(POST)
            sp.setQuery(query)
            sp.addCustomHttpHeader("User-Agent", UA)
            sp.addCustomHttpHeader("Accept", "application/sparql-results+json")
            sp.addParameter("timeout", str(hard_timeout_ms))
            sp.addParameter("maxlag", "5")
            sp.addParameter("format", "json")

            q = sp.query()
            resp = q.response

            headers = {}
            try:
                headers = dict(resp.info().items())
            except Exception:
                headers = {}
            raw = resp.read()

            ct = headers.get("Content-Type", "")
            ct_lower = ct.lower() if isinstance(ct, str) else str(ct).lower()
            looks_html = isinstance(raw, (bytes, bytearray)) and raw[:256].lstrip().startswith(b"<")
            if "text/html" in ct_lower or looks_html:
                snippet = (raw[:300].decode("utf-8", "replace") if isinstance(raw, (bytes, bytearray)) else str(raw))[:300]
                raise RuntimeError(f"Endpoint returned HTML, not JSON. CT={ct} Snippet={snippet!r}")
            
            if isinstance(raw, (bytes, bytearray)):
                return json.loads(raw.decode("utf-8"))
            if isinstance(raw, str):
                return json.loads(raw)
            
            return q.convert()
        
        except HTTPError as e:
            status = getattr(e, "code", None)
            retry_after = None
            try:
                retry_after = e.headers.get("Retry-After")
            except Exception:
                pass
            if status in (429, 502, 503, 504):
                if attempt >= max_retries:
                    raise
                if retry_after:
                    try:
                        wait_s = max(0.0, float(retry_after))
                    except Exception:
                        wait_s = None
                else:
                    wait_s = None
                if wait_s is None:
                    logging.warning(f"[{endpoint}] HTTP {status}; retrying (attempt {attempt}/{max_retries})")
                    _sleep_with_jitter(base_backoff, attempt)
                else:
                    logging.warning(f"[{endpoint}] HTTP {status} Retry-After={wait_s}; retrying (attempt {attempt}/{max_retries})")
                    import time as _t; _t.sleep(wait_s)
                last_err = e
                continue
            raise
        except (URLError, TimeoutError) as e:
            if attempt >= max_retries:
                raise
            logging.warning(f"[{endpoint}] Network/timeout; retrying (attempt {attempt}/{max_retries}): {e}")
            _sleep_with_jitter(base_backoff, attempt)
            last_err = e
            continue
        except (json.JSONDecodeError, RuntimeError) as e:
            if attempt >= max_retries:
                raise
            logging.warning(f"[{endpoint}] Bad payload; retrying (attempt {attempt}/{max_retries}): {e}")
            _sleep_with_jitter(base_backoff, attempt)
            last_err = e
            continue

    if last_err:
        raise last_err
    raise RuntimeError("SPARQL query failed with unknown error")

def coverage_from_sparql(
    endpoint: str, *,
    total_pages: int = 6,
    page_limit: int = 20_000,
    hll_p: int = 18,
    top_k: int = 10,
    cache_dir: str,
    graph: str = "",
) -> List[CoverageRow]:
        cache_file = compute_cache_filename(cache_dir, endpoint, graph or "any-graph", "sample")
        try:
            n_triples = collect_sample_to_nt(
                endpoint,
                cache_file,
                total_pages=total_pages,
                page_limit=page_limit,
                runner_kwargs=dict(max_retries=2, hard_timeout_ms=60_000),
                graph=graph,
            )
            if n_triples == 0:
                raise RuntimeError(f"No triples sampled from {endpoint} (graph={graph or 'ANY'})")

            rows_local: List[CoverageRow] = coverage_from_local(
                cache_file,
                hll_p=hll_p,
                sample=1.0,
                top_k=top_k
            )
            return rows_local
        except Exception: pass