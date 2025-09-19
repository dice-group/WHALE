from __future__ import annotations

import re
import math
import hashlib
from dataclasses import dataclass
from typing import Dict, List, Optional

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
    
    def add(self, value:bytes | str):
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