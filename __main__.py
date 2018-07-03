#!/usr/bin/env python3
import argparse
import os

from . import es_export
from . import es_import


def main():
	# creating a commande line arguments parser
	parser = argparse.ArgumentParser("py -m elasticsearch-scripts")
	subparsers = parser.add_subparsers(dest="subparser", help="Sub-modules")
	
	# creating the first subparser "export"
	parser_export = subparsers.add_parser("export", help="Module to export elasticsearch indexes")
	parser_export.add_argument("--batch-size", help="Number of documents per files", type=int, default=0)
	parser_export.add_argument("--directory", help="Folder where the indexes will be saved. Default value is current directory", type=str, default=f".{os.sep}")
	parser_export.add_argument("--filename", help="Filename for the indexes. Default value is 'index'", type=str, default="index")
	# setting up a group (either 'index' or '--dump-indexes' must be set)
	group = parser_export.add_mutually_exclusive_group()
	group.add_argument("--index", help="Index to export", type=str)
	group.add_argument("--dump-indexes", action='store_true', help="Dump all the indexes available")
	parser_export.add_argument("-v", "--verbosity", help="Enable verbose logging", action="store_true")
	
	# creating the second subparser "import"
	parser_import = subparsers.add_parser("import", help="Module to import indexes to elasticsearch")
	
	args = parser.parse_args()
	
	# return the status code of the submodule invoked
	if args.subparser == "export":
		return es_export.run(args)
	elif args.subparser == "import":
		return es_import.run(args)
	
	# if no submodule matched, return -1 (error code)
	return -1


if __name__ == '__main__':
	main()