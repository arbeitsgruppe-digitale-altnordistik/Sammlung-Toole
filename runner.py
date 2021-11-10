import subprocess
import sys

from util import utils

log = utils.get_logger(__name__)


def main() -> None:
    log.info("Runner started.")
    # load data from handrit
    log.info("Ensuring that handrit data is available")
    try:
        subprocess.run("git submodule update --init --remote --recursive".split(), check=True)
        log.info("Handrit.is data loaded")
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
        log.exception("An unexpected exception occured while running the app.")
        code = 1
    log.info("Shut down.")
    sys.exit(code)


if __name__ == "__main__":
    main()
