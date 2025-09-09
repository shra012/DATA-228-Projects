#!/usr/bin/env python3
import sys
import re

word_re = re.compile(r"[A-Za-z0-9']+")
for line in sys.stdin:
    for w in word_re.findall(line.lower()):
        print(f"{w}\t1")
