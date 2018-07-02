#!/usr/bin/env python3
import elasticsearch
import argparse
import colorama
import os

from . import es_export
from . import es_import


def main():
	parser = argparse.ArgumentParser()
	subparsers = parser.add_subparsers(dest="subparser", help="Sub-modules")
	
	parser_export = subparsers.add_parser("export", help="Module to export elasticsearch indexes")
	parser_export.add_argument("--batch-size", help="Number of documents per files", type=int, default=0)
	parser_export.add_argument("--start", help="", type=str, default="")
	parser_export.add_argument("--directory", help="Folder where the indexes will be saved. Default value is current directory", type=str, default=f".{os.sep}")
	parser_export.add_argument("--filename", help="Filename for the indexes. Default value is 'index'", type=str, default="index")
	parser_export.add_argument("--start-id", help="", type=str, default="")
	
	parser_import = subparsers.add_parser("import", help="Module to import indexes to elasticsearch")
	
	args = parser.parse_args()
	
	if args.subparser == "export":
		return es_export(args)
	elif args.subparser == "import":
		return es_import(args)
	
	return -1

if __name__ == '__main__':
	main()