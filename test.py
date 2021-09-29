from util import datahandler


def main():
    handler = datahandler.DataHandler.get_handler()
    print(handler.manuscripts.head())
    print(handler.manuscripts.columns)


if __name__ == "__main__":
    main()
