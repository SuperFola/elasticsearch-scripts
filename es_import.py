#!/usr/bin/env python3
import elasticsearch as es
import colorama
from colorama import Fore, Style
import os
import configparser as cfgparser
import pprint
from datetime import datetime
import glob
import ast
import time


# initialize colorama
colorama.init()
pimp = lambda color: print(color, end='')
unpimp = lambda: print(colorama.Style.RESET_ALL, end='')
pimpit = lambda color, *args, **kwargs: [pimp(color), print(*args, **kwargs), unpimp(), None][-1]

# debugging tools
log = lambda mode, end=' ': print(f"[{datetime.now().time()}] {mode}:", end=end)
convert_task_index_to_text = lambda index: _tasks[index]

# pretty printer useful pretty much in all the script
pprinter = pprint.PrettyPrinter(indent=4)
pp = lambda *args, **kwargs: pprinter.pprint(*args, **kwargs)

# read config from current working directory
config = cfgparser.ConfigParser()
config.read("elasticsearch.ini")


def import_(db, args):
	if not hasattr(import_, "i"):
		import_.i = 0
	if not hasattr(import_, "tasks"):
		import_.tasks = [
			"checking if wanted index is free for use",
			"loading mappings and settings",
			"creating index",
			"searching files to import",
			"assembling files to recreate an index",
			"inserting data into index",
			"optimizing index"
		]
	if not hasattr(import_, "task_to_string"):
		import_.task_to_string = lambda: import_.tasks[import_.i - 1]
	
	# checks if wanted index is free for use
	if db.indices.exists(args.index):
		log("ERROR"); pimpit(Fore.RED, f"Index '{args.index}' already exists, aborting")
		raise Exception("Choose another name for the index to create")
	if args.verbosity:
		log("INFO"); pimpit(Fore.CYAN, f"Index '{args.index}' is free for use")
	yield
	import_.i += 1
	
	# loads	mappings and settings
	information = {}
	info_filename = f"{args.directory}{os.sep}index-{args.filename}.json"
	if not os.path.exists(info_filename):
		log("ERROR"); pimpit(Fore.RED, f"The mapping+setting file 'index-{args.filename}.json' can not be found")
		raise Exception("Retry to export the wanted index")
	with open(info_filename, encoding="utf-8") as file:
		information = ast.literal_eval(file.read())
	yield
	import_.i += 1
	
	# creates index
	db.indices.create(args.index, {
		"settings": information["settings"]
		, "mappings": information["mappings"]
	})
	yield
	import_.i += 1
	
	# searches files to import
	files = [f for f in glob.glob(f"{args.directory}{os.sep}*") if os.path.isfile(f) and os.path.basename(f)[:len(args.filename)] == args.filename]
	if args.verbosity:
		log("INFO"); pimpit(Fore.CYAN, f"Found {len(files)} file(s)")
	if len(files) == 0:
		log("ERROR"); pimpit(Fore.RED, f"No file(s) matching given pattern '{args.filename}' at {args.directory}")
		raise Exception("Double check if the files exist at the given location, and try again")
	yield
	import_.i += 1
	
	# re-assembles the file(s) to recreate an index
	tmp_index = []
	for filename in files:
		with open(filename, encoding="utf-8") as file:
			tmp_index += ast.literal_eval(file.read())
	if args.verbosity:
		log("INFO"); pimpit(Fore.CYAN, f"Index created from files has a size of {len(tmp_index)} elements")
	yield
	import_.i += 1
	
	# inserts data into index
	if args.verbosity:
		log("INFO"); pimpit(Fore.CYAN, f"Putting aliases for index '{args.index}'")
	for alias in information["aliases"].keys():
		db.indices.put_alias(alias, args.index)
	
	if args.verbosity:
		log("INFO"); pimpit(Fore.CYAN, f"Importing the documents for index '{args.index}'")
	for document in tmp_index:
		db.index(args.index, document["_type"], document["_source"], document["_id"])
	
	# checking if the right amount of data was exported
	db.indices.flush(args.index)
	time.sleep(2)
	total_hits = db.search(args.index, search_type="count")["hits"]["total"]
	if total_hits == len(tmp_index):
		log("INFO"); pimpit(Fore.GREEN, f"All the documents were imported into the index '{args.index}'")
	else:
		log("ERROR"); pimpit(Fore.RED, f"{len(tmp_index) - total_hits} document(s) missing in index")
		raise Exception("Try to import the index again")
	
	# open the index to make it available for search
	db.indices.open(args.index)
	if args.verbosity:
		log("INFO"); pimpit(f"Index '{args.index}' opened to make it available for search")
	yield
	import_.i += 1
	
	# optimizing index (only if wanted)
	if config.getboolean("General", "optimize index after creating"):
		if args.verbosity:
			log("INFO"); pimpit(Fore.CYAN, f"Optimizing index '{args.index}'")
		db.indices.optimize(args.index)
		if args.verbosity:
			log("INFO"); pimpit(Fore.CYAN, f"Index '{args.index}' optimized")
	yield
	import_.i += 1


def run(args):
	"""
	Syntax: py -m MODULE import
		[-r|--recursive]
		[--directory path]
		filename
		index
		[-v|--verbosity]
	"""
	
	log("INFO"); pimpit(Fore.CYAN, "Starting")
	
	# connecting to elasticsearch
	db = es.Elasticsearch(
		[
			{
				'host': config.get(host, "name")
				, 'port': config.get(host, "port")
				, 'use_ssl': config.getboolean(host, "ssl")
			}
			for host in config.sections() if host[:4] == "Host"
		]
		, verify_certs=config.getboolean("General", "verify certs")
		, ca_certs=config.get("General", "ca certs")
		, sniff_on_start=config.getboolean("Misc", "sniff on start")
	)
	
	# debug information on start
	if args.verbosity:
		log("INFO (Elasticsearch)", end="\n"); pimp(Fore.GREEN); pp(db.info()); unpimp()
		log("INFO (configuration)"); pimpit(Fore.CYAN, f"{config.get('Misc', 'info')}")
	
	# checking if the db is running
	if db.info()['status'] != 200:
		pimpit(Fore.RED, f"We have a problem Houston ! Elastic search is returning {db.info()['status']} instead of 200 OK!\n"
				"Contact the server administrator or double check the configuration of the script")
		return -1
	
	# checking if wanted directory exists
	if args.directory and not os.path.exists(args.directory):
		log("ERROR"); pimpit(Fore.RED, f"{args.directory} does not exist, can not find wanted files")
		return -1
	
	# importing indexes
	try:
		for _ in import_(db, args):
			pass
		log("INFO"); pimpit(Fore.CYAN, "Done")
		return 0
	except Exception as e:
		log("ERROR"); print("A critical error occurred. It will be displayed below (for you to be able to fix it)")
		log("INFO "); print(f"The program stopped at : {import_.task_to_string().title()}")
		
		log(type(e).__name__); pimpit(Fore.RED, e)
		return -1