#!/usr/bin/env python3
"""
Simulate an Ollama NDJSON token stream for testing kq's real-time emit.

Each JSON object in RECORDS is broken into single-character fragments,
emitted as {"response":"<char>"} envelopes with a configurable delay.
A short pause between objects makes it easy to observe kq emitting
each matched record before the next object starts.

Usage:
  python3 testdata/slow_llm_stream.py | ./zig-out/bin/kq --llm response 'select name, dept where dept = "eng"'
  python3 testdata/slow_llm_stream.py --delay 0.02 | ./zig-out/bin/kq --llm response 'select name, score where score > 80'

Options:
  --delay <secs>   Delay between tokens (default: 0.04)
  --pause <secs>   Pause between objects  (default: 0.4)
"""

import sys
import json
import time

RECORDS = [
    {"name": "Alice",   "dept": "eng",   "score": 92},
    {"name": "Bob",     "dept": "sales", "score": 75},
    {"name": "Carol",   "dept": "eng",   "score": 88},
    {"name": "Dave",    "dept": "sales", "score": 61},
    {"name": "Eve",     "dept": "eng",   "score": 95},
    {"name": "Frank",   "dept": "ops",   "score": 70},
    {"name": "Grace",   "dept": "eng",   "score": 83},
    {"name": "Heidi",   "dept": "ops",   "score": 55},
]

delay = 0.04
pause = 0.4

i = 1
while i < len(sys.argv):
    if sys.argv[i] == "--delay" and i + 1 < len(sys.argv):
        delay = float(sys.argv[i + 1]); i += 2
    elif sys.argv[i] == "--pause" and i + 1 < len(sys.argv):
        pause = float(sys.argv[i + 1]); i += 2
    else:
        i += 1

for rec in RECORDS:
    text = json.dumps(rec)
    for ch in text:
        envelope = json.dumps({"response": ch})
        print(envelope, flush=True)
        time.sleep(delay)
    time.sleep(pause)

# terminal envelope matching Ollama's done marker
print(json.dumps({"response": "", "done": True}), flush=True)
