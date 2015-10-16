#!/usr/bin/env python

"""Parse RRD XML dumps, i.e. the output of rrdtool dump $file.rrd
Particulary written for munin RRDs.

Code adapted from Ben Godfrey, https://gist.github.com/afternoon/947301
"""

# need lxml, stdlib xml.etree ignores comments
from lxml import etree
from sys import argv


class RRDException(Exception):
    pass


def comment_content(c):
    """Return the inner content from an XML comment. Strips surrounding
    whitespace.

    >>> comment_content("<!-- Yay! -->")
    'Yay!'

    """
    content = str(c)[4:-3]
    return content.strip()


def get_ts(c):
    """Return the unix timestamp component of an RRD XML date comment.

    >>> get_ts("<!-- 2011-04-28 19:18:40 BST / 1304014720 -->")
    '1304014720'

    """
    date, tstamp = comment_content(c).split("/")
    return tstamp.strip()


def _values(db):
    row_nodes = db.xpath("./row")
    for rn in row_nodes:
        yield (v.text for v in rn)


def _timestamps(db):
    """Extract timestamps from comments."""
    timestamp_nodes = db.xpath("./comment()")
    return (get_ts(c) for c in timestamp_nodes)


def convert_value(s):
    """Takes a string value, returns a contained number, or the original string"""
    try:
        return float(s)
    except ValueError:
        return s

def cdps(tree):
    """From the lxml tree of an RRD dump file, yield each CDP as tuple of
    - consolidation function (MIN, MAX, AVERAGE, LAST)
    - timestamp
    - value
    """
    # munin RRDs should only have one DS each, but better check
    ds_ = tree.findall('./ds')
    if len(ds_) is not 1:
        raise RRDException(
                "Munin RRD expected to have only one DS, found %d" % len(ds_))

    for rra in tree.findall('./rra'):
        cf = rra.find('cf').text
        db = rra.find('database')

        # yield each cdp (row)
        for timestamp, row_values in zip(_timestamps(db), _values(db)):
            value_list = list(row_values)
            # check again for more than one DS, should not happen
            if len(value_list) is not 1:
                raise RRDException(
                        "Munin RRD expected to have only one DS, " +
                        "but found CDP with values %d" % value_list)
            # convert_value(timestamp), because they are epoch, so just integers, too
            yield (cf, int(convert_value(timestamp)), convert_value(value_list[0]))


if __name__ == "__main__":
    # for testing only
    rrdfile = argv[1]
    tree = etree.parse(rrdfile)
    for cdp in cdps(tree):
        print(cdp)
