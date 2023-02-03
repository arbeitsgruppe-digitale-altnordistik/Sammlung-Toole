import argparse

from ops.build import build, update_and_build


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--no-update", "-n",
        action="store_true",
        help="Do not update the handrit.is git submodule. this is useful if the DB should contain a particular version of te data"
    )
    # LATER: we could also add an option to pass a git hash, and it would automatically check that version out
    args = parser.parse_args()
    if args.no_update:
        build()
    else:
        update_and_build()


if __name__ == "__main__":
    main()
