#!/usr/bin/env python
"""Export CSV from an RRD XML dump.

Usage: rrdxml.py file.xml rra

Where rra is the 0-based index for the RRA you want to dump.

"""
from csv import writer
from lxml.etree import parse
from sys import argv, stdout


def dump(f, rra):
    """Dump RRA to list of lists."""
    tree = parse(f)
    yield [s.strip() for s in tree.xpath("//ds/name/text()")]
    for row in tree.xpath("//rra[%s]/database/row" % (rra + 1)):
        yield [v.text for v in row]


def dump_csv(f, rra, out):
    """Dump RRA to CSV (written to file object out)."""
    w = writer(out)
    for row in dump(f, rra):
        w.writerow([s.strip() for s in row])


if __name__ == "__main__":
    dump_csv(argv[1], int(argv[2]), stdout)