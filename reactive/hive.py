import jujuresources
from charms.reactive import when, when_not
from charms.reactive import set_state, remove_state, is_state
from charmhelpers.core import hookenv
from subprocess import check_call
from charmhelpers.fetch import apt_install

def dist_config():
    from jujubigdata.utils import DistConfig  # no available until after bootstrap

    if not getattr(dist_config, 'value', None):
        hive_reqs = ['vendor', 'hadoop_version', 'packages', 'groups', 'users', 'dirs', 'ports']
        dist_config.value = DistConfig(filename='dist.yaml', required_keys=hive_reqs)
    return dist_config.value


@when_not('bootstrapped')
def bootstrap():
    hookenv.status_set('maintenance', 'Installing base resources')

    # Hive cannot handle - in the metastore db name and mysql uses the service name to name the db
    if "-" in hookenv.service_name():
        hookenv.status_set('blocked', 'Service name should not contain -. Redeploy with a different name.')
    else:
        set_state('hive.valid')

    apt_install(['python-pip', 'git'])  # git used for testing unreleased version of libs
    check_call(['pip', 'install', '-U', 'pip'])  # 1.5.1 (trusty) pip fails on --download with git repos
    mirror_url = hookenv.config('resources_mirror')
    if not jujuresources.fetch(mirror_url=mirror_url):
        missing = jujuresources.invalid()
        hookenv.status_set('blocked', 'Unable to fetch required resource%s: %s' % (
            's' if len(missing) > 1 else '',
            ', '.join(missing),
        ))
        return
    set_state('bootstrapped')


@when('hive.valid')
@when_not('hive.installed')
def install_hive(*args):
    from charms.hive import Hive  # in lib/charms; not available until after bootstrap

    hive = Hive(dist_config())
    if hive.verify_resources():
        hookenv.status_set('maintenance', 'Installing Apache Hive')
        hive.install()
        set_state('hive.installed')

@when('hive.valid')
@when_not('database.connected')
def missing_mysql():
    from charms.hive import Hive  # in lib/charms; not available until after bootstrap

    hive = Hive(dist_config())
    hive.new_db_connection()
    hookenv.status_set('blocked', 'Waiting for relation to database')

@when('hive.valid', 'database.connected')
@when_not('database.available')
def waiting_mysql(mysql):
    hookenv.status_set('waiting', 'Waiting for database to become ready')

@when('hive.valid')
@when_not('hadoop.connected')
def missing_hadoop():
    hookenv.status_set('blocked', 'Waiting for relation to Hadoop')

@when('hive.valid', 'hadoop.connected')
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

    if not is_state('database.available') and not is_state('hadoop.ready'):
        hookenv.status_set('blocked', 'Waiting for haddop and database connections')
    elif not is_state('database.available'):
        hookenv.status_set('blocked', 'Waiting for database connection')
    elif not is_state('hadoop.ready'):
        hookenv.status_set('blocked', 'Waiting for Haddop connection')
    else:
        hookenv.status_set('blocked', 'Hive stopped')

