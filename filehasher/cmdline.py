#!/usr/bin/env python

from .filehasher import FileHasher
from .version import __version__
import sys
import argparse

DEFAULT_FILENAME = ".hashes"


def main():
    parser = argparse.ArgumentParser(description=f'File Hasher v{__version__}.')

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

    fh = FileHasher(args.hashfile)

    if args.generate:
        fh.generate_hashes(append=False)
    elif args.append:
        fh.generate_hashes(append=True)
    elif args.update:
        fh.generate_hashes(update=True)
    elif args.compare:
        fh.compare(args.compare)
    else:
        parser.print_help()

    sys.exit(0)


if __name__ == "__main__":
    main()
