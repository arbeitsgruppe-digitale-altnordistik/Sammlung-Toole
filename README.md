# Sammlung-Toole

A new and shiny look on data from [Handrit.is](https://handrit.is/).


## Usage

Be sure to have Python installed.
Optionally, `pipenv` or `make` can be used.
Install all requirements (`pipenv install` or `pip install -r requirements.txt`).


### Running the web App

To run the web app locally, execute `pipenv run run` or `streamlit run src/Home.py`. 
Once the app is started up, it should automatically open up in the browser. 
If not, follow the link displayed in the terminal 
(normally `http://localhost:8501`).


### Re-building the Database

To re-build the database from the handrit.is XML files, 
execute `pipenv run rebuild` or `python src/rebuild.py`.

This will initialize and update the git submodule containing all the XML data form handrit.is, 
before building the database content from those XML files.

If this is not the intended behaviour, 
e.g., if you have checked out a particular version of the handrit data,
from which you want to build the database, 
execute `pipenv run rebuild --no-update` or `python src/rebuild.py --no-update`.


## Development

### Dev Setup

It is recommended to use pipenv for virtual environment management.

As an IDE, Visual Studio Code is recommended.

To minimize errors, linting with `mypy` is recommended. This will enforce strict typing, which can be a major source of bugs in python code.  
To minimize formatting differences (and therewith noise in the git diffs), `autopep8` is recommended as a formatter.  
Both linter and formatter can be activated in VS Code.

### Dependencies and Automation

#### Adding dependencies with pipenv

To add a new dependency, run

```shell
pipenv install <package-name>
```

To add a dependency that's only needed for development, run

```shell
pipenv install --dev <package-name>
```

#### Create the requirements file

Whenever dependencies have been added, the requirements files should be updated. Run

```shell
pipenv lock --requirements > requirements.txt
```

and

```shell
pipenv lock --dev-only --requirements > dev-requirements.txt
```

#### Makefile

There is a `Makefile` to automate some tasks with the GNU Make commandline tool.  
To execute make targets, you may need a UNIX-style terminal. On Linux and MacOS this should work out of the box. 
On Windows you may try with Git Bash or some other UINX-type shell.

To execute a make target, open the root of the repository in your terminal and run `make <target-name>` (where `<target-name>` should be replaced with a available target name).  
To see the available targets, simply run `make help` or just `make`, which will give you a list of the commands and a short description, if available.

### Documentation

The documentation page can be found on the [repo Github pages](https://arbeitsgruppe-digitale-altnordistik.github.io/Sammlung-Toole/)

The docs are created using `mkdocs` with the `material` theme.  
All documentation is created from the markdown files in the `docs/` directory. Table of content, configurations, etc. are defined in the `mkdocs.yml` file in the project root.

To work on the docs, simply modify the the markdown files in the `docs/` directory.  
To see a live preview of the docs, run `pipenv run docs-serve` and then visit [http://127.0.0.1:8000/](http://127.0.0.1:8000/) in your browser. (The docs will update whenever you save the files.)

The docs ara automatically deployed to the Github pages whenever a branch is merged to `main`.

### Manually updating the data with Git submodules

The data used in this application is pulled from [Handrit.is on GitHub](https://github.com/Handrit/Manuscripts). 
The Handrit/Manuscript repository is embedded in the present repo as a Git submodule.
In order to initialize the submodule, you should execute `pipenv run init` before you run the tool for the first time.
To keep your local data up to date, you can execute `pipenv run update`.
