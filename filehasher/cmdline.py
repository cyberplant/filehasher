#!/usr/bin/env python

import filehasher
import sys
import argparse

DEFAULT_FILENAME = ".hashes"


def main():
    parser = argparse.ArgumentParser(description='File Hasher.')

    parser.add_argument('--generate', '-g', action='store_true',
                        dest="generate",
                        help="generate hashes (remove hashfile if exists)")

    parser.add_argument('--append', '-a', action='store_true',
                        dest="append",
                        help="Append hashes to hashfile")

    parser.add_argument('--update', '-u', action='store_true',
                        dest="update",
                        help="Update hashfile "
                             "(clean old enties and append new)")

    parser.add_argument('--compare', '-c', nargs='?',
                        dest="compare", default=False,
                        help="Compare hashes from hashfiles. You can use this "
                             "command to check dupes.")

    parser.add_argument('hashfile', default='.hashes',
                        nargs='?',
                        help="Hashes file. Default filename: %(default)s")

    args = parser.parse_args()

    if args.generate:
        filehasher.generate_hashes(args.hashfile)
    elif args.append:
        filehasher.generate_hashes(args.hashfile, append=True)
    elif args.update:
        filehasher.generate_hashes(args.hashfile, update=True)
    elif args.compare:
        filehasher.compare(args.hashfile, args.compare)

    sys.exit(0)


if __name__ == "__main__":
    main()
