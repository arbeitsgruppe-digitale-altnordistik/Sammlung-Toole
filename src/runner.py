import subprocess
import sys
import argparse

from lib.utils import get_logger
from ops.build import build_db

log = get_logger(__name__)


def main() -> None:
    parser = argparse.ArgumentParser(description="Runs the web app, unless the --build flag is chosen, in which case the DB will be rebuilt.")
    parser.add_argument("--build", "-b", "-db", action="store_true", help="Wipe and re-build the database from the Handrit.is XML files")
    args = parser.parse_args()
    if args.build:
        build_db()
    else:
        run()


def run() -> None:
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
