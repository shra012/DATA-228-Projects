#!/usr/bin/env python3
import sys

current = None
count = 0
for line in sys.stdin:
    word, n = line.rstrip("\n").split("\t", 1)
    n = int(n)
    if word != current:
        if current is not None:
            print(f"{current}\t{count}")
        current, count = word, n
    else:
        count += n
if current is not None:
    print(f"{current}\t{count}")
