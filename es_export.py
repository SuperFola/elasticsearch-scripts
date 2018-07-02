#!/usr/bin/env python3
import elasticsearch as es
import colorama
from colorama import Fore, Style
import os
import configparser as cfgparser
import pprint
from datetime import datetime


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


def export(db, args):
	if not hasattr(export, "i"):
		export.i = 0
	if not hasattr(export, "tasks"):
		export.tasks = [
			"initializing",
			"finding indices",
			"retrieving wanted index",
			"preparing data for saving",
			"saving data to disk"
		]
	if not hasattr(export, "task_to_string"):
		export.task_to_string = lambda: export.tasks[export.i - 1]
	
	# initializing
	if args.verbosity:
		log("INFO"); pimpit(Fore.CYAN, f"{db.count()}")
	yield
	export.i += 1
	
	# find all the indices
	indices = [i.strip() for i in db.cat.indices(h='index').split("\n") if i]
	if args.verbosity or args.dump_indexes:
		log("INFO"); pimpit(Fore.CYAN, f"{indices}")
		if args.dump_indexes:
			return
	yield
	export.i += 1
	
	# retrieving data of the wanted index
	# ...
	yield
	export.i += 1
	
	# preparing data for saving to file(s) (cut in multiple lists of correct size (--batch-size X), etc)
	# ...
	yield
	export.i += 1
	
	# save data to file(s)
	# ...
	yield


def run(args):
	"""
	Syntax: py -m MODULE export
		[--batch-size size>0]
		[--directory name]
		[--filename name]
		(--index name|--dump-indexes)
		[-v|--verbosity]
	"""
	
	if not args.index and not args.dump_indexes:
		log("ERROR"); pimpit(Fore.RED, "You must give either an index to save to disk or ask to dump the indexes available")
		return -1
	
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
		pimp(Fore.RED)
		print(f"We have a problem Houston ! Elastic search is returning {db.info()['status']} instead of 200 OK!\n"
				"Contact the server administrator or double check the configuration of the script")
		unpimp()
		return -1
	
	# creating needed directories
	if args.directory and args.directory != f".{os.sep}":
		if args.verbosity:
			log("INFO"); pimpit(Fore.CYAN, f"Creating directories for '{args.directory}'")
		os.makedirs(args.directory, exist_ok=True)
	
	# exporting indexes
	try:
		for _ in export(db, args):
			pass
		return 0
	except Exception as e:
		log("ERROR"); print("A critical error occurred. It will be displayed below (for you to be able to fix it)")
		log("INFO "); print(f"The program stopped at : {export.task_to_string().title()}")
		
		pimp(Fore.RED); log(type(e).__name__); unpimp()
		print(e)
		return -1