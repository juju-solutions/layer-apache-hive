from charms.reactive import when, when_not
from charms.reactive import set_state, remove_state
from charmhelpers.core import hookenv

def dist_config():
    from jujubigdata.utils import DistConfig  # no available until after bootstrap

    if not getattr(dist_config, 'value', None):
        hive_reqs = ['vendor', 'hadoop_version', 'packages', 'groups', 'users', 'dirs', 'ports']
        dist_config.value = DistConfig(filename='dist.yaml', required_keys=hive_reqs)
    return dist_config.value

@when('bootstrapped')
@when_not('hive.installed')
def install_hive(*args):
    from charms.hive import Hive  # in lib/charms; not available until after bootstrap

    hive = Hive(dist_config())
    if hive.verify_resources():
        hookenv.status_set('maintenance', 'Installing Apache Hive')
        hive.install()
        set_state('hive.installed')

@when('bootstrapped')
@when_not('database.connected')
def missing_mysql():
    hookenv.status_set('blocked', 'Waiting for relation to database')

@when('bootstrapped', 'database.connected')
@when_not('database.available')
def waiting_mysql(mysql):
    hookenv.status_set('waiting', 'Waiting for database to become ready')

@when('bootstrapped')
@when_not('hadoop.connected')
def missing_hadoop():
    hookenv.status_set('blocked', 'Waiting for relation to Hadoop')

@when('bootstrapped', 'hadoop.connected')
@when_not('hadoop.ready')
def waiting_hadoop(hadoop):
    hookenv.status_set('waiting', 'Waiting for Hadoop to become ready')


@when('hive.installed', 'hadoop.ready', 'database.available')
@when_not('hive.started')
def start_hive(*args):
    from charms.hive import Hive  # in lib/charms; not available until after bootstrap

    hookenv.status_set('maintenance', 'Setting up Apache Hive')
    hive = Hive(dist_config())
    hive.setup_hive_config()
    hive.configure_hive(args[1])
    hive.start()
    set_state('hive.started')
    hookenv.status_set('active', 'Ready')


@when('hive.started')
@when_not('hadoop.ready', 'database.available')
def stop_hive():
    from charms.hive import Hive  # in lib/charms; not available until after bootstrap

    hookenv.status_set('maintenance', 'Stopping Apache Hive')
    hive = Hive(dist_config())
    hive.stop()
    remove_state('hive.started')


