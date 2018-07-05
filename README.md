# Readme

## Installation

You will need Python 3 (recommended version : 3.7) and a computer under Windows.

To install the libraries, just `cd` in `elasticsearch-scripts/` and launch `configure.bat`

## Backup.py

The script `backup.py` is useful in order to create backups of the whole database, under folders named `backup-date-time/INDEX_NAME`.

No arguments are required to start this script.

## Importing/Exporting data from/to Elasticsearch

The command is : `py -m elasticsearch-scripts MODE ARGUMENTS`

`MODE` is either `import` or `export`

`ARGUMENTS` changes regarding the `MODE`

Documentation of each mode below.

### Importing

```
C:\Users\plateau\Desktop\stuff\git>py -m elasticsearch-scripts export --help
usage: py -m elasticsearch-scripts export [-h] [--batch-size BATCH_SIZE]
                                          [--directory DIRECTORY]
                                          [--filename FILENAME] [-q]
                                          [--index INDEX | --dump-indexes]
                                          [-v]

optional arguments:
  -h, --help            show this help message and exit
  --batch-size BATCH_SIZE
                        Number of documents per files
  --directory DIRECTORY
                        Folder where the indexes will be saved. Default value
                        is current directory
  --filename FILENAME   Filename for the indexes. Default value is 'index'
  -q, --quiet           Silently answers 'yes' to any question
  --index INDEX         Index to export
  --dump-indexes        Dump all the indexes available
  -v, --verbosity       Enable verbose logging
```

#### Examples

```
> py -m elasticsearch-scripts export --batch-size 400 --directory bunch_of_folders/created/automatically --filename a_nice_filename --index zbibale --quiet
> py -m elasticsearch-scripts export --index zbibale --verbosity
> py -m elasticsearch-scripts export --dump-indexes
```

### Exporting

```
C:\Users\plateau\Desktop\stuff\git>py -m elasticsearch-scripts import --help
usage: py -m elasticsearch-scripts import [-h] [--directory DIRECTORY] [-q]
                                          [-v]
                                          filename index

positional arguments:
  filename              Specify the pattern of filename
  index                 Which index should be created based on the given files

optional arguments:
  -h, --help            show this help message and exit
  --directory DIRECTORY
                        Folder where the indexes will be saved. Default value
                        is current directory
  -q, --quiet           Silently answers 'yes' to any question
  -v, --verbosity       Enable verbose logging
```

#### Examples

```
> py -m elasticsearch-scripts import --verbosity --directory test\first\second zbibale test_index
> py -m elasticsearch-scripts import zbibale test_index
```