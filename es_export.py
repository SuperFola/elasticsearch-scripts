#!/usr/bin/env python3
import elasticsearch as es
import colorama
from colorama import Fore, Style
import os
import configparser as cfgparser
import pprint
from datetime import datetime
import glob
import math


# initialize colorama
colorama.init()
pimp = lambda color: print(color, end='')
unpimp = lambda: print(colorama.Style.RESET_ALL, end='')
pimpit = lambda color, *args, **kwargs: [pimp(color), print(*args, **kwargs), unpimp(), None][-1]

# debugging tools
log = lambda mode, end=' ': print(f"[{datetime.now().time()}] {mode}:", end=end)
convert_task_index_to_text = lambda index: _tasks[index]
count_digits = lambda x: math.ceil(math.log10(x))

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
			"retrieving number of documents from the wanted index",
			"getting documents",
			"preparing data for saving",
			"saving data to disk"
		]
	if not hasattr(export, "task_to_string"):
		export.task_to_string = lambda: export.tasks[export.i]
	
	# initializing
	if args.verbosity:
		log("INFO"); pimpit(Fore.CYAN, f"GET _count => {db.count()}")
	yield
	export.i += 1
	
	# find all the indices
	indices = [i.strip() for i in db.cat.indices(h='index').split("\n") if i]
	if args.verbosity or args.dump_indexes:
		log("INFO"); pimpit(Fore.CYAN, f"GET _cat/indices => {indices}")
		if args.dump_indexes:
			return
	yield
	export.i += 1
	
	# retrieve number of documents from the wanted index
	data_count = db.search(args.index, search_type="count")
	if args.verbosity:
		log("INFO"); pimpit(Fore.CYAN, f"GET /{args.index}/search?search_type=count => {data_count}")
	total_hits = data_count["hits"]["total"]
	yield
	export.i += 1
	
	# get documents from the wanted index (if no batch-size specified,
	#   retrieve everything per batch of 256 (can be change in config file))
	batch_size = config.get("General", "batch size", fallback=0) if args.batch_size == 0 else args.batch_size
	try:
		batch_size = int(batch_size)
	except ValueError:
		batch_size = 0
		log("ERROR"); pimpip(Fore.RED, "The default batch size given in the configuration file is not an integer")
	if batch_size == 0:
		log("ERROR"); pimpit(Fore.RED, "Batch size can not be equal to 0")
		raise Exception("Fix either the command or the configuration file")
	batch_count = total_hits // batch_size + (1 if total_hits % batch_size else 0)
	data = []
	for i in range(batch_count):
		for content in db.search(args.index, from_=i * batch_size, size=batch_size)["hits"]["hits"]:
			data.append(content["_source"])
	if args.verbosity:
		log("INFO"); pimpit(Fore.CYAN, f"Retrieved {len(data)} documents from index {args.index}")
	yield
	export.i += 1
	
	# prepare data for saving to file(s) (cut in multiple lists of correct size (--batch-size X), etc)
	split_data = []
	if args.batch_size != 0:
		# the user wants to split the dat in multiple files
		split_data = []
		current = []
		for i, d in enumerate(data):
			current.append(d)
			if len(current) == args.batch_size:
				if args.verbosity:
					log("INFO"); pimpit(Fore.CYAN, f"Batch no {len(split_data)} contains {len(current)} documents")
				split_data.append(current)
				current = []
				
		if current:
			if args.verbosity:
				log("INFO"); pimpit(Fore.CYAN, f"Batch no {len(split_data)} contains {len(current)} documents")
			split_data.append(current)
			current = []
		
	yield
	export.i += 1
	
	# save data to file(s)
	if args.batch_size != 0:
		# save to multiple files
		cwd = os.getcwd()
		os.chdir(args.directory)
		for i, content in enumerate(split_data):
			i_zeropadded = str(i).zfill(count_digits(len(split_data)))
			with open(f"{args.index}.{i_zeropadded}.json", "w", encoding="utf-8") as file:
				file.write(str(content))
		os.chdir(cwd)
	else:
		cwd = os.getcwd()
		os.chdir(args.directory)
		with open(f"{args.index}.json", "w", encoding="utf-8") as file:
			file.write(str(data))
		os.chdir(cwd)
	if args.verbosity:
		log("INFO"); pimpit(Fore.GREEN, f"Index {args.index} saved to disk at {args.directory}{os.sep}{args.index}[.x].json")
	yield
	export.i += 1


def run(args):
	"""
	Syntax: py -m MODULE export
		[--batch-size size>0]
		[--directory name]
		[--filename name]
		[-q|--quiet]
		(--index name|--dump-indexes)
		[-v|--verbosity]
	"""
	
	if not args.index and not args.dump_indexes:
		log("ERROR"); pimpit(Fore.RED, "You must give either an index to save to disk or ask to dump the indexes available")
		return -1
	
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
	
	# creating needed directories
	if args.directory and args.directory != f".{os.sep}":
		if args.verbosity:
			log("INFO"); pimpit(Fore.CYAN, f"Creating directories for '{args.directory}'")
		os.makedirs(args.directory, exist_ok=True)
		if glob.glob(f"{args.directory}{os.sep}*"):
			if not args.quiet:
				log("WARNING"); pimpit(Fore.YELLOW, f"Directory {args.directory} is not empty !")
				log("INFO"); pimpit(Fore.CYAN, "Would you like to continue ? If so, the directory will be wiped out.") 
				ans = ""
				while ans.lower().strip() not in ('no', 'yes', 'n', 'y', 'non', 'oui', 'n', 'o', '0', '1'):
					log("Q"); ans = input("[yes|no] > ")
				if ans.lower().strip() in ('no', 'n', 'non', 'n', '0'):
					log("INFO"); pimpit(Fore.CYAN, "Aborting.")
					return 0
			for file in glob.glob(f"{args.directory}{os.sep}*"):
				os.remove(file)
	
	# exporting indexes
	try:
		for _ in export(db, args):
			pass
		log("INFO"); pimpit(Fore.CYAN, "Done")
		return 0
	except Exception as e:
		log("ERROR"); print("A critical error occurred. It will be displayed below (for you to be able to fix it)")
		log("INFO "); print(f"The program stopped at : {export.task_to_string().title()}")
		
		log(type(e).__name__); pimpit(Fore.RED, e)
		return -1