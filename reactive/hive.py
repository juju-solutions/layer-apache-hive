from charms.reactive import when, when_not, when_none
from charms.reactive import set_state, remove_state
from charmhelpers.core import hookenv
from charms.hive import Hive
from charms.hadoop import get_dist_config


@when_not('hadoop.related')
def report_blocked():
    hookenv.status_set('blocked', 'Waiting for relation to Hadoop Plugin')


@when('hadoop.installed')
@when_not('hive.installed')
def install_hive(hadoop):
    hookenv.status_set('maintenance', 'Installing base resources')

    # Hive cannot handle - in the metastore db name and
    # mysql uses the service name to name the db
    if "-" in hookenv.service_name():
        hookenv.status_set('blocked', 'Service name should not contain -. Redeploy with a different name.')
        return False

    hive = Hive(get_dist_config())
    if hive.verify_resources():
        hookenv.status_set('maintenance', 'Installing Apache Hive')
        hive.install()
        set_state('hive.installed')
        return True

    return False


@when('hive.installed')
@when_none('hive.started', 'database.connected')
def missing_mysql():
    hive = Hive(get_dist_config())
    hive.new_db_connection()
    hookenv.status_set('blocked', 'Waiting for relation to database')


@when('hive.installed', 'database.connected')
@when_none('hive.started', 'database.available')
def waiting_mysql(mysql):
    hookenv.status_set('waiting', 'Waiting for database to become ready')


@when('hive.installed', 'database.available')
@when_none('hive.started', 'hadoop.ready')
def waiting_hadoop(db):
    hookenv.status_set('waiting', "Waiting for HDFS to become ready")


@when('hive.installed', 'hadoop.ready', 'database.available')
@when_not('hive.started')
def start_hive(hdfs, database):
    hookenv.status_set('maintenance', 'Setting up Apache Hive')
    hive = Hive(get_dist_config())
    hive.setup_hive_config()
    hive.configure_hive(database)
    hive.open_ports()
    hive.start()
    set_state('hive.started')
    hookenv.status_set('active', 'Ready')


@when('hive.started')
@when_none('hadoop.ready', 'database.available')
def stop_hive():
    hookenv.status_set('maintenance', 'Stopping Apache Hive')
    hive = Hive(get_dist_config())
    hive.stop()
    hive.close_ports()
    remove_state('hive.started')
    hookenv.status_set('blocked', 'Waiting for Hadoop and database connections')


@when('hive.started', 'hadoop.ready')
@when_not('database.available')
def stop_hive_wait_db(hdfs):
    hookenv.status_set('maintenance', 'Stopping Apache Hive')
    hive = Hive(get_dist_config())
    hive.stop()
    hive.close_ports()
    remove_state('hive.started')
    hookenv.status_set('blocked', 'Waiting for database connection')


@when('hive.started', 'database.available')
@when_not('hadoop.ready')
def stop_hive_wait_hdfs(db):
    hookenv.status_set('maintenance', 'Stopping Apache Hive')
    hive = Hive(get_dist_config())
    hive.stop()
    hive.close_ports()
    remove_state('hive.started')
    hookenv.status_set('blocked', 'Waiting for Hadoop connection')


@when('hive.installed', 'client.joined')
def client_joined(hive):
    dist = get_dist_config()
    port = dist.port('hive')
    hive.send_port(port)
    hive.set_ready()
    set_state('client.configured')


@when('hive.installed', 'client.configured')
@when_not('client.joined')
def client_departed():
    remove_state('client.configured')
