from charms.reactive import when, when_not, when_none
from charms.reactive import set_state, remove_state
from charmhelpers.core import hookenv
from charms.hadoop import get_hadoop_base
from jujubigdata.handlers import HDFS
from jujubigdata.utils import DistConfig
from charms.hive import Hive

DIST_KEYS = ['vendor', 'hadoop_version', 'packages', 'groups', 'users', 'dirs', 'ports']

def get_dist_config(keys):
    '''
    Read the dist.yaml. Soon this method will be moved to hadoop base layer.
    '''
    if not getattr(get_dist_config, 'value', None):
        get_dist_config.value = DistConfig(filename='dist.yaml', required_keys=keys)
    return get_dist_config.value


@when('hadoop.installed')
@when_not('hive.installed')
def validate_deployment():
    hookenv.status_set('maintenance', 'Installing base resources')

    # Hive cannot handle - in the metastore db name and mysql uses the service name to name the db
    if "-" in hookenv.service_name():
        hookenv.status_set('blocked', 'Service name should not contain -. Redeploy with a different name.')
        return False

    hive = Hive(get_dist_config(DIST_KEYS))
    if hive.verify_resources():
        hookenv.status_set('maintenance', 'Installing Apache Hive')
        hive.install()
        set_state('hive.installed')
        return True
    
    return False


@when('hive.installed')
@when_none('hive.started','database.connected')
def missing_mysql():
    hive = Hive(get_dist_config(DIST_KEYS))
    hive.new_db_connection()
    hookenv.status_set('blocked', 'Waiting for relation to database')


@when('hive.installed', 'database.connected')
@when_none('hive.started','database.available')
def waiting_mysql(mysql):
    hookenv.status_set('waiting', 'Waiting for database to become ready')


@when('hive.installed')
@when_none('hive.started', 'hdfs.related')
def missing_hadoop():
    hookenv.status_set('blocked', 'Waiting for relation to HDFS')


@when('hive.installed', 'hdfs.related')
@when_none('hive.started','hdfs.ready')
def waiting_hadoop(hdfs):
    base_config = get_hadoop_base()
    hdfs.set_spec(base_config.spec())
    hookenv.status_set('waiting', "Waiting for HDFS to become ready")


@when('hive.installed', 'hdfs.related', 'hdfs.spec.mismatch')
@when_not('hdfs.ready')
def spec_missmatch_hadoop(*args):
    hookenv.status_set('blocked', "We have a spec mismatch between the underlying HDFS and the charm requirements")

@when('hive.installed', 'hdfs.ready', 'database.available')
@when_not('hive.started')
def start_hive(hdfs, database):

    hookenv.status_set('maintenance', 'Setting up Hadoop base files')
    base_config = get_hadoop_base()
    hadoop = HDFS(base_config)
    hadoop.configure_hdfs_base(hdfs.ip_addr(), hdfs.port())

    hookenv.status_set('maintenance', 'Setting up Apache Hive')
    hive = Hive(get_dist_config(DIST_KEYS))
    hive.setup_hive_config()
    hive.configure_hive(database)
    hive.start()
    set_state('hive.started')
    hookenv.status_set('active', 'Ready')


@when('hive.started')
@when_none('hdfs.ready', 'database.available')
def stop_hive():
    hookenv.status_set('maintenance', 'Stopping Apache Hive')
    hive = Hive(get_dist_config(DIST_KEYS))
    hive.stop()
    remove_state('hive.started')
    hookenv.status_set('blocked', 'Waiting for Haddop and database connections')


@when('hive.started', 'hdfs.ready')
@when_not('database.available')
def stop_hive_wait_db(hdfs):
    hookenv.status_set('maintenance', 'Stopping Apache Hive')
    hive = Hive(get_dist_config(DIST_KEYS))
    hive.stop()
    remove_state('hive.started')
    hookenv.status_set('blocked', 'Waiting for database connection')


@when('hive.started', 'database.available')
@when_not('hdfs.ready')
def stop_hive_wait_hdfs(db):
    hookenv.status_set('maintenance', 'Stopping Apache Hive')
    hive = Hive(get_dist_config(DIST_KEYS))
    hive.stop()
    remove_state('hive.started')
    hookenv.status_set('blocked', 'Waiting for Haddop connection')

