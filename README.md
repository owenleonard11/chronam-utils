# Generating Chronicling America
This repository contains modules for interacting with the [Chronicling America](https://chroniclingamerica.loc.gov/) database of historical newspapers, in support of an upcoming project on post-OCR correction and retrieval-augmented generation.

## Project Overview
Generating Chronicling provides an alternative to the Chronicling America [advanced]() and [basic]() search interfaces. Its main advantages over the existing options are:
- **Enhanced querying**. While the official search interfaces are limited to a single query at a time, Generating Chronicling can instead retrieve results for groups of related queries. This is useful if, for example, we want to retrieve 10 results each from multiple states across multiple years. `usage.py` shows examples of this feature.
- **Procedural download**. Although Chronicling America provides batch downloads for OCR data, downloading and unzipping large archives is inefficient when we want a smaller sample that is distributed across multiple batches. The methods provided by the `ChronAmDownloader` class allow for arbitrary groups of resources to be downloaded without incurring additional overhead.
- **Parallelization**. Both query retrieval and and file download are designed to be used with multithreading while still respecting rate limits.

## Quickstart Guide
> **NOTE**: This guide assumes that you have git, [Python](https://www.python.org/) 3.9 or greater, and [pip](https://pypi.org/project/pip/) 21.0 or greater installed. Generating Chronicling may work with older versions of Python and pip, but has not been tested.

Start here to get up and running with the basics of Generating Chronicling. Once you've downloaded the code and installed the requirements, you can also follow along in the Jupyter notebook at `notebooks/quickstart.ipynb`.

### Download & Installation
To download the code, run the following from the directory in which you want Generating Chronicling to be installed:
```
git clone git@github.com:owenleonard11/generating-chronicling.git
```
This should create a new folder called `generating-chronicling`. To continue with installation, switch into the new folder:
```
cd generating-chronicling
```
To manage the Python packages required to run Generating Chronicling, you'll want to create a new Python vitual environment:
```
python -m venv env
```
Next, activate the virtual environment. This step may depend on your operating system.

**Windows Users**: 
```
env\Scripts\activate
```

**Unix/MacOS Users**: 
```
source env/bin/activate
```
Now, simply run the following to install the required packages using pip:
```
pip install -r requirements.txt
```
If you intend to use Jupyter notebooks, also install the dev requirements:
```
pip install -r requirements-dev.txt
```

### Getting Started
To get started, create a new notebook in the `generating-chronicling` directory:
```
touch quickstart.ipynb
```
You can also follow along in the provided `noteboks/quickstart_notebook.ipynb`, or simply run `python` to start an interactive Python instance.

First, import the `query`, `limit`, and `download` modules and initialize a `ChronAmRateLimiter`:
```python
from modules.query import *
from modules.limit import *
from modules.download import *

limiter = ChronAmRateLimiter()
```
The `limiter` object will keep track of timestamps to make sure that queries and downloads don't exceed the limits set by the Library of Congress newspaper API. If your IDE prompts you to select a kernel, select the virtual environment you created during **Download & Installation**—it should be called `env`.

### Making a Query
The easiest way to initialize a query is to use [Chronicling America Search interface](https://chroniclingamerica.loc.gov/#tab=tab_search). After choosing a state, range of years, and search terms, click "Go" and wait for the results. Once the results page appears, copy the entire URL and paste it into the `ChronAmQuery.from_url()` initializer:
```python
query = ChronAmBasicQuery.from_url('<YOUR/URL/HERE>')
```
If initialization has succeeded, you should be able to see your choices as properties of the object by running
```python
print(query)
```
To retrieve a set of results for the query, use `query.retrieve_all()`. The first argument is the number of results to retrieve per page, and the second argument is the `limiter` we defined earlier:
```python
query.retrieve_all(25, limiter)
```
By default, queries retrieve a maximum of 50 results; the code above will thus retrieve 2 pages of 25 results each. Retrieval may take some time, since the Chronicling America API has to filter and search over 21 million pages. If the query is successful, you should see something like the following:
```
INFO: found 21072 results for query 139746080010400
INFO: updated query "139746080010400" with 50 items.
```
We can also view the results stored in `query.results`:
```python
list(query.results.values())[:5]
```

At first, query results are stored in memory and will be lost if Python is interrupted. To store the results in a file, use the `dumpt_txt()` method:
```python
query.dump_txt('data/query.txt')
```
This will write the results as a newline-separated list of IDs to the file at `data/query.txt`—if this gives an error, make sure that the `data` directory exists.

### Downloading Files
Now that we have a list of IDs, we can download the associated files. We first initialize a `ChronAmDownloader` from the file we wrote our results to:
```python
loader = ChronAmDownloader.from_file('data/query.txt', 'data/files/', limiter)
```
Here `data/files/` is the directory to which the files will be downloaded—you'll have to make the folder exists first. Note that`limiter` is the `ChronAmRateLimiter` that we have already defined—this is important, since querying and downloading share a rate limit. You can check the length of `loader.ids` to make sure that the query results were property loaded:
```python
list(loader.ids.keys())[:5]
```
The result should look the same as the result from running `list(query.results.values())[:5]`. 

To download all of the text files associated with the query results, simply use
```python
loader.download_all('txt')
```
Once the download has been completed, you can use the `check_downloads()` method to see how many files have been downloaded relative to the total number of results from the query:
```python
loader.check_downloads('txt')
```
And now the files should be viewable in `data/files/`. We can see the list of downloaded files by calling
```python
print(loaders.paths)
```
We can also check out the first 10 lines of one of the text files:
```python
with open(loader.paths[0], 'r') as fp:
    for i in range(10):
        print(fp.readline())
```
