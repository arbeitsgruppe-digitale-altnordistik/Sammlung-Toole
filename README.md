# Sammlung-Toole

Some useful tools for digital Old Norse studies.


## Getting started

Follow these steps, in order to be able to use the `Sammlung Toole`.

1. Be sure to have Python installed.
   - to verify your python installation, run `python --version` in the terminal. This should return a version number, not an error message.
2. Ensure you have `pipenv` installed.
   - to check if you you have it installed, run `pip list` and see if it's in the list.
   - if you don't have it installed, run `pip install pipenv`
3. Let pipenv install everything necessary for you
   - normally running `pipenv install` in a terminal that's opened in the root folder of this repository, should do.
   - if you want to develop, you'll also need the dev-dependencies installed, so run `pipenv install --dev` instead.
4. Run the script.
   - simply run `pipenv run run` for normal use cases. This will start the tool.
   - if you need more specific command, run `pipenv run <your-command>`  
     (e.g. `pipenv run python -m streamlit run interface.py --server.port 80`)
   - if you want to run multiple commands within the pipenv context, run `pipenv shell`. This will turn your terminal into a pipenv shell until you execute `exit`. (I.e. all commands will behave as if they had the perfix `pipenv run`.)


All commands that need running, should be executed from a terminal/command-line-interface that is opened in the root folder of this repo (i.e. the same folder as this file is located).

To open a terminal in this folder, use one of the following options:

- Open the folder in windows explorer, and in the navigation bar, replace the file path with `cmd` and hit enter.
- In Github Desktop, click `Repository > Open in Command Prompt`.
- Open the folder in VS Code and click `View > Terminal`.
- Download and install "Windows Terminal", which enables you to right-click on a folder and select `Open in Windows Terminal`.
- Open a command prompt and navigate to the desired folder using the command `cd` (google how it works).


## Development

### Dev Setup

It is recommended to use pipenv for virtual environment management.

As an IDE, Visual Studio Code is recommended.

To minimize errors, linting with `mypy` is recommended. This will enforce strict typing, which can be a major source of bugs in python code.  
To minimize formatting differences (and therewith noise in the git diffs), `auopep8` is recommended as a formatter.  
Both linter and formatter can be activated in VS Code.

### Adding dependencies with pipenv

To add a new dependency, run

```shell
pipenv install <package-name>
```

To add a dependency that's only needed for development, run

```shell
pipenv install --dev <package-name>
```

### Create the requirements file

Whenever dependencies have been added, the requirements files should be updated. Run

```shell
pipenv lock --requirements > requirements.txt
```

and

```shell
pipenv lock --dev-only --requirements > dev-requirements.txt
```
