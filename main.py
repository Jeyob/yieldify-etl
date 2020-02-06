# standard libraries
import gzip
import argparse
import textwrap
import logging
import configparser
import sys
import pyhocon
from app import db_connect, top_os_by_unique_users, top_browsers_by_unique_users, top_cities_by_events, top_countries_by_events
from pathlib import Path
import sqlite3

# external libraries

# TODO create methods for stdout or API

logger = logging.getLogger(__name__)


def setup_logging():
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S',
                        filename='main.log',
                        filemode='a')

    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.INFO)
    formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
    console.setFormatter(formatter)
    logging.getLogger(__name__).addHandler(console)
    return logging.getLogger(__name__)


def process_config(args):
    """validate and load config file"""
    config_file = Path(args.config_file)
    if not config_file.is_file():
        raise FileNotFoundError(f"configuration file {config_file} not found")
    try:
        config = pyhocon.ConfigFactory.parse_file(config_file)
        args.config = config
    except Exception as e:
        raise ValueError(f"Could not parse config file {config_file}: {str(e)}")


def run_stat(args, run_type):
    logger.info("running stats jobs")

    config = pyhocon.ConfigFactory.parse_file(args.config_file)
    conn = db_connect(config.connections.databases.sqlite.path)
    conn.isolation_level = None

    stat_jobs = [("Top countries by events", top_countries_by_events),
                 ("Top cities by events", top_cities_by_events),
                 ("Top browsers by unique users", top_browsers_by_unique_users),
                 ("Top OS by unique users", top_os_by_unique_users)]

    try:
        with conn:
            for title, func in stat_jobs:
                print(title)
                result = func(conn)
                i = 1
                for name, count in result:
                    print(f"{i}. {name} : {count}")
                print()
    except sqlite3.Error as e:
        logger.error(f"{__name__}: error ", exc_info=True)
        sys.exit(1)
    finally:
        conn.close()


def rebuild_database(args):
    """Populate database with tables"""
    logger.info(f"(Re)building database...")

    sql_files = ["schema.sql"]

    config = pyhocon.ConfigFactory.parse_file(args.config_file)
    conn = db_connect(config.connections.databases.sqlite.path)
    conn.isolation_level = None  # Autocommit
    try:
        with conn:
            cursor = conn.cursor()
            for s in sql_files:
                logger.info(f"  Running sql/{s}...")
                cursor.executescript(Path(f"sql/{s}").read_text())
        logger.info("Database (re)build completed.")
    except sqlite3.Error as e:
        logger.error(f"{__name__}: error running sql script", exc_info=True)
        sys.exit(1)
    finally:
        conn.close()


if __name__ == '__main__':

    argparser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                                        description='Calculates stats..',
                                        epilog=textwrap.dedent('''\
                                        ------------------------------------------------------
                                        If ran as API, supported endpoints are:
                                          1) /stats/browser   % breakdown of detected browsers
                                          2) /stats/os        % breakdown of detected OS"
                                          3) /stats/device    % breakdown of device type
                                          '''))

    argparser.add_argument('-c', '--config-file', default='main.conf', metavar='main.conf', type=str,
                           required=True, help='path to config file (default: main.conf')
    argparser.add_argument('-l', '--log-file', default='main.log', metavar='main.log', help='path to logfile (default: main.log)')
    subparsers = argparser.add_subparsers()
    rebuild_parser = subparsers.add_parser('rebuild-database')
    rebuild_parser.set_defaults(func=lambda args: rebuild_database(args))
    run_parser = subparsers.add_parser('run')
    run_parser.add_argument('-t', '--run-type', choices=['api', 'stdout'], default='stdout',
                            required=True, help='where output will be provided')
    run_parser.set_defaults(func=lambda args: run_stat(args, args.run_type))

    # configure logging
    logger = setup_logging()
    # parse commandline arguments
    args = argparser.parse_args()
    # read config
    process_config(args)
    # run commands
    args.func(args)
