#!/usr/bin/env python3
# Identity reducer: Hadoop sorts by key; we just print "nb \t count"
import sys
for line in sys.stdin:
    parts = line.rstrip("\n").split("\t", 2)
    if len(parts) == 3:
        _, c, nb = parts
        print(f"{nb}\t{c}")
