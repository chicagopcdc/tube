import tube.settings as config
from pyspark import SparkConf, SparkContext


def get_sql_to_hdfs_config(config):
    return {
        'input': {
            'jdbc': config['JDBC'],
            'username': config['DB_USERNAME'],
            'password': config['DB_PASSWORD'],
        },
        'output': config['HDFS_DIR']
    }


def list_to_file(lst, file_path):
    with open(file_path, 'w') as f:
        f.write('\n'.join(lst))


def make_spark_context(config):
    '''
    Makes a spark and sqlContext
    '''
    conf = SparkConf().setAppName(config.APP_NAME)
    if config.RUNNING_MODE == 'Dev':
        # We should only use the value of `config.spark_master` in
        # a test context. Production runs need to set the Spark Master
        # to 'yarn'. This is done in the arguments to `spark-submit`.
        conf = conf.setMaster(config.SPARK_MASTER)
    sc = SparkContext(conf=conf, pyFiles=[])

    # Configure logging
    log4j = sc._jvm.org.apache.log4j
    log4j.LogManager.getRootLogger().setLevel(log4j.Level.FATAL)

    return sc


def get_hdfs_file_handler(sc=None):
    if sc is None:
        sc = make_spark_context(config)
    uri = sc._gateway.jvm.java.net.URI
    opath = sc._gateway.jvm.org.apache.hadoop.fs.Path
    file_system = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
    fs = file_system.get(uri("hdfs://localhost:9000"), sc._jsc.hadoopConfiguration())
    return fs, opath


def make_sure_hdfs_path_exist(path, sc=None):
    fs, opath = get_hdfs_file_handler(sc)
    if not fs.exists(opath(path)):
        fs.mkdirs(opath(path))
    return path
