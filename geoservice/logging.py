import datetime
import logging
import sys
from contextlib import contextmanager

from colorama import Fore, Style, just_fix_windows_console

ROOT_LOGGER = logging.getLogger()
FORMATTER = logging.Formatter(
    "%s[{asctime} | {timedelta} Δs]%s "
    "%s@{name:<16} %s"
    "{levelname} "
    ":: {indent}"
    "{message} %s%s" % (Fore.CYAN, Fore.RESET, Fore.BLUE, Fore.RESET, Style.RESET_ALL, Fore.RESET),
    # "({filename}:{lineno})" % (Fore.CYAN, Fore.RESET),
    style='{'
)
INDENT = False


class TimedeltaFilter(logging.Filter):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.previous_entry_timestamp = None

    def filter(self, record):
        previous_entry_timestamp = self.previous_entry_timestamp \
            if self.previous_entry_timestamp \
            else record.relativeCreated
        # - - - - - - - - - - - - - - - - - - - -
        timedelta = (
            datetime.datetime.fromtimestamp(record.relativeCreated/1e3)
            - datetime.datetime.fromtimestamp(previous_entry_timestamp/1e3)
        )
        # - - - - - - - - - - - - - - - - - - - -
        record.timedelta = f'{(timedelta.seconds + timedelta.microseconds/1e6):.2f}'
        record.timedelta = record.timedelta.rjust(6)
        # - - - - - - - - - - - - - - - - - - - -
        self.previous_entry_timestamp = record.relativeCreated
        return True


class IndentFilter(logging.Filter):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def filter(self, record):
        record.indent = '-> ' if INDENT else ''
        return True


def setup_logging(loglevel: str, debug: bool = False):
    logging.addLevelName(logging.DEBUG, f"{Style.DIM}{logging.getLevelName(logging.DEBUG).ljust(8)}")
    logging.addLevelName(logging.INFO, f"{Fore.GREEN}{logging.getLevelName(logging.INFO).ljust(8)}")
    logging.addLevelName(logging.WARNING, f"{Fore.YELLOW}{logging.getLevelName(logging.WARNING).ljust(8)}")
    logging.addLevelName(logging.ERROR, f"{Fore.RED}{logging.getLevelName(logging.ERROR).ljust(8)}")
    logging.addLevelName(logging.CRITICAL, f"{Style.BRIGHT}{Fore.RED}{logging.getLevelName(logging.CRITICAL).ljust(8)}")
    setup_logging_to_console(debug)
    ROOT_LOGGER.setLevel(loglevel)


def setup_logging_to_console(debug: bool = False):
    just_fix_windows_console()
    console_handler = logging.StreamHandler(sys.stdout)
    if debug:
        console_handler.addFilter(TimedeltaFilter())
        console_handler.addFilter(IndentFilter())
        console_handler.setFormatter(FORMATTER)
    ROOT_LOGGER.addHandler(console_handler)


@contextmanager
def all_logging_disabled(highest_level=logging.CRITICAL):
    previous_level = logging.root.manager.disable
    logging.disable(highest_level)
    try:
        yield
    finally:
        logging.disable(previous_level)


@contextmanager
def logger_indent():
    global INDENT
    try:
        INDENT = True
        yield
    finally:
        INDENT = False
