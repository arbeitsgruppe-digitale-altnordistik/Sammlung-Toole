import subprocess
import sys

from lib.utils import GitUtil, get_logger

log = get_logger(__name__)


def initialize() -> None:
    args = "git submodule init".split()
    subprocess.run(args, check=True)


def update() -> None:
    args = "git submodule update --remote".split()
    subprocess.run(args, check=True)
    log.info("Updated data from handrit")
    # store info that submodule has been updated
    GitUtil.update_submodule_state()


def is_up_to_date() -> bool:
    args = "git -C data/handrit diff HEAD origin/master --shortstat".split()
    process = subprocess.run(args, capture_output=True, check=True)
    output = str(process.stdout, 'utf-8').strip()
    numbers = [int(s) for s in output.split() if s.isdigit()]
    res = not bool(numbers and numbers[0])
    log.info(f"Checking if handrit data is up to date evaluated: {res}")
    log.debug(f"Git diff output: {output if output else 'None'}")
    return res


def main() -> None:
    # update submodule data
    log.info("Runner started... checking submodule status")
    try:
        initialize()
        if not is_up_to_date():
            update()
        if not is_up_to_date():
            log.warning("Data is not up to date despite trying to update")
    except Exception:
        log.exception("Failed to load Handrit.is data from github")

    # run streamlit app
    try:
        log.info("Starting Streamlit app...")
        # The following try-except is a very inelegant solution to the problem of pipenv not being on path
        try:
            process = subprocess.run("pipenv run python -m streamlit run src/gui/Home.py".split())
        except Exception:
            log.debug("pipenv not on path?")
            process = subprocess.run("python3 -m pipenv run python -m streamlit run src/gui/Home.py".split(), shell=True)
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
