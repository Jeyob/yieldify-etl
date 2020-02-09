# standard libraries
import gzip
import argparse
import textwrap
import logging
import sys
import pyhocon
from app import *
from pathlib import Path
import sqlite3
import os
from app import create_app
# external libraries



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


def stats_to_stdout(config_file: str):
    config = pyhocon.ConfigFactory.parse_file(config_file)
    conn = db_connect(config.connections.databases.sqlite.path)
    conn.isolation_level = None

    stat_jobs = [("Top countries by events", TopCountriesByEvents(conn)),
                 ("Top cities by events", TopCitiesByEvents(conn)),
                 ("Top browsers by unique users", TopBrowsersByUniqueUsers(conn)),
                 ("Top OS by unique users", TopOSByUniqueUsers(conn))]
    try:
        with conn:
            for title, func in stat_jobs:
                print(title)
                result = func.execute(db_conn=conn)
                for i, value in enumerate(result, 1):
                    print(f"{i}. {value[0]}")
                print()
    except sqlite3.Error as e:
        logger.error(f"{__name__}: error ", exc_info=True)
        sys.exit(1)
    finally:
        conn.close()


def stats_to_api(config_file):
    """
    Run a webserver to receive requests and return stats through webapi
    :param config_file:
    :return:
    """
    app = create_app()
    app.run(host='localhost', port=8080, debug=False)


def run_stat(args, run_type: str):
    """
    Calculates the stats and prints to stdout
    :param args: parsed command line arguments
    :param run_type: parameter provided to command choices = ('api' or 'stdout')
    :return:
    """
    logger.info("running stats jobs")

    if run_type == 'stdout':
        stats_to_stdout(args.config_file)
    elif run_type == 'api':
        stats_to_api(args.config_file)
    else:
        raise ValueError(f"run_type parameter {run_type} not recognised")


def rebuild_database(config_file, input_file):
    """Populate database with tables"""
    logger.info(f"(Re)building database...")

    sql_table_files = ["schema.sql"]
    sql_index_files = ["index.sql"]

    config = pyhocon.ConfigFactory.parse_file(config_file)
    conn = db_connect(config.connections.databases.sqlite.path)
    #conn.isolation_level = None  # Autocommit
    input_file = input_file
    try:
        with conn:
            cursor = conn.cursor()
            for s in sql_table_files:
                logger.info(f"  Running sql/{s}...")
                cursor.executescript(Path(f"sql/{s}").read_text())
            logger.info("Database tables (re)build completed.")

            logger.info("Loading input file to db...")
            transform_and_load(input_file, conn, config.connections.databases.ip2location.path)
            logger.info("Database load completed")

            for s in sql_index_files:
                logger.info(f"  Running sql/{s}...")
                cursor.executescript(Path(f"sql/{s}").read_text())
            logger.info("Database index (re)build completed")

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
    rebuild_parser.add_argument('inputfile', type=str, help="input file in gzip format")
    rebuild_parser.set_defaults(func=lambda x: rebuild_database(args.config_file, args.inputfile))
    run_parser = subparsers.add_parser('run')
    run_parser.add_argument('-t', '--run-type', choices=['api', 'stdout'], default='stdout',
                            required=True, help='where output will be provided')
    run_parser.set_defaults(func=lambda x: run_stat(args, args.run_type))

    # configure logging
    logger = setup_logging()
    # parse commandline arguments
    args = argparser.parse_args()
    # set config environment variable
    os.environ["YIELDIFY_CONFIG"] = str(Path(args.config_file).absolute())
    # read config
    process_config(args)
    # run commands
    args.func(args)
