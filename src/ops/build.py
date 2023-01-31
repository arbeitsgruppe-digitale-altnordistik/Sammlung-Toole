import subprocess

from lib.utils import get_logger

log = get_logger(__name__)


def initialize() -> None:
    args = "git submodule init".split()
    subprocess.run(args, check=True)


def update() -> None:
    args = "git submodule update --remote".split()
    subprocess.run(args, check=True)
    log.info("Updated data from handrit")


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
    pass


if __name__ == "__main__":
    main()
