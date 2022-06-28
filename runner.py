import subprocess
import sys

from util import utils
from util.utils import GitUtil
from util import database
from util import tamer

log = utils.get_logger(__name__)


def initialize() -> None:
    args = "git submodule init".split()
    subprocess.run(args, check=True)


def update() -> None:
    args = "git submodule update --remote".split()
    subprocess.run(args, check=True)
    log.info("Updated data from handrit")
    # store info that submodule has been updated
    GitUtil.update_submodule_state()
    # LATER: could determin which files changed so that only those need to be re-parsed


def isUpToDate() -> bool:
    args = "git -C data/handrit diff HEAD origin/master --shortstat".split()
    process = subprocess.run(args, capture_output=True, check=True)
    output = str(process.stdout, 'utf-8').strip()
    numbers = [int(s) for s in output.split() if s.isdigit()]
    res = not bool(numbers and numbers[0])
    log.info(f"Checking if handrit data is up to date evaluated: {res}")
    log.debug(output)
    return res


def main() -> None:
    # update submodule data
    try:
        initialize()
        if not isUpToDate():
            update()
        if not isUpToDate():
            log.warning("Data is not up to date despite trying to update")
    except Exception:
        log.exception("Failed to load Handrit.is data from github")

    # run streamlit app
    try:
        log.info("Starting Streamlit app...")
        process = subprocess.run("pipenv run python -m streamlit run interface.py".split())
        log.info(f"Process terminated with status code: {process.returncode}")
        code = process.returncode
    except KeyboardInterrupt:
        log.info("App stopped with keyboard interrupt")
        code = 0
    except Exception:
        log.exception("An unexpected error occured while running the app.")
        code = 1
    log.info("Shut down.")
    sys.exit(code)


if __name__ == "__main__":
    main()
