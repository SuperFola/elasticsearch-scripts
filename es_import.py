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


def import_(db, args):
	if not hasattr(import_, "i"):
		import_.i = 0
	if not hasattr(import_, "tasks"):
		import_.tasks = [
			"initializing"
		]
	if not hasattr(import_, "task_to_string"):
		import_.task_to_string = lambda: import_.tasks[import_.i - 1]
	
	# initializing
	if args.verbosity:
		log("INFO"); pimpit(Fore.CYAN, f"{db.count()}")
	yield
	import_.i += 1
	
	yield


def run(args):
	"""
	Syntax: py -m MODULE import
		[-r|--recursive]
		[-p|--path path]
		index_name
		[-v|--verbosity]
	"""
	
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
		for _ in import_(db, args):
			pass
		return 0
	except Exception as e:
		log("ERROR"); print("A critical error occurred. It will be displayed below (for you to be able to fix it)")
		log("INFO "); print(f"The program stopped at : {export.task_to_string().title()}")
		
		pimp(Fore.RED); log(type(e).__name__); unpimp()
		print(e)
		return -1