import subprocess
import sys

from lib.utils import get_logger

log = get_logger(__name__)


def main() -> None:
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
