import argparse
import tube.settings as config

from tube.spark import make_spark_context
from tube.spark.translator import Gen3Translator
from cdislogging import get_logger

logger = get_logger(__name__)


def main():
    '''
    Define the spark context and parse agruments into config
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config',
                        help='The configuration set to run with',
                        type=str,
                        choices=['Test', 'Dev', 'Prod'],
                        default='Dev')
    parser.add_argument("-v", "--verbose",
                        help="increase output verbosity",
                        action="store_true")

    args = parser.parse_args()

    config.RUNNING_MODE = args.config

    sc = make_spark_context(config)

    etl = Gen3Translator(sc, config)
    etl.run_etl()

    # Tear down actions
    sc.stop()


if __name__ == '__main__':
    # Execute Main functionality
    main()
