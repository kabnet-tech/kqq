#!/usr/bin/env python3
"""Generate a ~1 GB JSON file: {"records":[...]}"""
import json, random, string, sys, os

def rand_str(n=8):
    return ''.join(random.choices(string.ascii_lowercase, k=n))

target = 1 * 1024 * 1024 * 1024  # 1 GiB
out = sys.stdout.buffer
out.write(b'{"records":[\n')
written = len('{"records":[\n')
i = 0
first = True
while written < target:
    rec = {
        "id":     i,
        "name":   rand_str(8),
        "score":  round(random.uniform(0, 100), 2),
        "active": random.choice([True, False]),
        "city":   rand_str(6),
        "tags":   [rand_str(4) for _ in range(3)],
    }
    line = json.dumps(rec, separators=(',', ':'))
    if not first:
        out.write(b',\n')
        written += 2
    out.write(line.encode())
    written += len(line)
    first = False
    i += 1
out.write(b'\n]}\n')
sys.stderr.write(f"Generated {i} records, ~{written/1024/1024:.0f} MB\n")
