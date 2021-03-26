# Sammlung-Toole

Some useful tools for digital Old Norse studies.


## Getting started

Follow these steps, in order to be able to use the `Sammlung Toole`.

1. Be sure to have Python installed.
2. Install all required packages.
3. Run the script.
<!-- TODO: How to run it - streamlit -->

All commands that need running, should be executed from a terminal/command-line-interface that is opened in the root folder of this repo (i.e. the same folder as this file is located).

To open a terminal in this folder, use one of the following options:

- Open the folder in windows explorer, and in the navigation bar, replace the file path with `cmd` and hit enter.
- In Github Desktop, click `Repository > Open in Command Prompt`.
- Open the folder in VS Code and click `View > Terminal`.
- Download and install "Windows Terminal", which enables you to right-click on a folder and select `Open in Windows Terminal`.
- Open a command prompt and navigate to the desired folder using the command `cd` (google how it works).


## Requirements

### Install all requirements

To install all required packages, simply run

```
pip install -r requirements.txt
```

This will install all packages listed in the file `requirements.txt`.


### Install single package

Install single packages using the command

```
pip install <package-name>
```

e.g.

```
pip install lxml
```

If a package needs to be installed, this most likely means that it's required. If so, please add it to `requirements.txt`.


## Run the tool

From the directory containing this repository, run `python -m streamlit run interface.py`
This will start everything neccessary and open the interface in your default browser. It will also
display how to access the interface (local and remote URL). When running certain parts of the 
script(s), it will show their CLI output there as well.



## Getting Data Manually

Use the `crawler.py` to get data.  
On top of your python file, add `import crawler`, then you can access methids from the crawler with e.g. `crawler.load_xml(url)`.

It's best to cache everything locally, so you get data quickly. See below for more information.

The following functions are meant to be used:

- `load_xml(url)`
- `load_xml_by_filename(filename)`
- `load_xmls_by_id(idno)`

More can be added at any point.


## Crawling Data

The crawler by default caches data locally, storing information in CSV files and saving all the XML files in a separate folder.

It's best to crawl the intire set of data at the beginning, so afterwards, operations run a lot quicker.  
Be sure to repeat this step, so that your cached data is up to date.  
To cache everything, call

```python
import crawler
crawler.crawl()
```

You can do more specific tasks with the following functions:

- `get_collections()`
- `get_ids()`
- `get_xml_urls()`
- `cache_all_xml_data()`
- `get_shelfmarks()`

For details on usage and parameters, see the docstring. (In VSCode, hover over the function after typing it, and you'll get a popup. Otherwise, look at the comments in the code.)
