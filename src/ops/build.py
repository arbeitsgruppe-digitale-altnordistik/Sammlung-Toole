import subprocess

from lib.utils import get_logger
from ops.db_init import db_init

log = get_logger(__name__)


def initialize() -> None:
    args = "git submodule init".split()
    subprocess.run(args, check=True)
    log.info("Ensured that the git submodule is initialized")


def update() -> None:
    args = "git submodule update --remote".split()
    subprocess.run(args, check=True)
    log.info("Updated data from handrit")


def build_db() -> None:
    initialize()
    update()
    db_init()
