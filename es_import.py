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
			"searching files to import",
			"assembling files to recreate an index"
		]
	if not hasattr(import_, "task_to_string"):
		import_.task_to_string = lambda: import_.tasks[import_.i - 1]
	
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