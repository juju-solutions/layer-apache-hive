# pylint: disable=unused-argument
from charms.reactive import when, when_not, when_not_all
from charms.reactive import is_state, set_state, remove_state
from charmhelpers.core import hookenv
from charms.layer.apache_hive import Hive
from charms.layer.hadoop_client import get_dist_config


@when('hadoop.ready')
@when_not('hive.installed')
def install_hive(hadoop):
    # Hive cannot handle - in the metastore db name and
    # mysql uses the service name to name the db
    if "-" in hookenv.service_name():
        hookenv.status_set('blocked', 'Service name should not contain -. '
                                      'Redeploy with a different name.')
        return

    hive = Hive(get_dist_config())
    if hive.verify_resources():
        hookenv.status_set('maintenance', 'Installing Apache Hive')
        hive.install()
        set_state('hive.installed')


@when('hive.installed')
def report_status():
    hadoop_joined = is_state('hadoop.joined')
    hadoop_ready = is_state('hadoop.ready')
    database_joined = is_state('database.connected')
    database_ready = is_state('database.available')
    if not hadoop_joined and not database_joined:
        hookenv.status_set('blocked',
                           'Waiting for relation to database and Hadoop')
    elif not hadoop_joined:
        hookenv.status_set('blocked',
                           'Waiting for relation to Hadoop')
    elif not database_joined:
        hookenv.status_set('blocked',
                           'Waiting for relation to database')
    elif not hadoop_ready and not database_ready:
        hookenv.status_set('waiting',
                           'Waiting for database and Hadoop')
    elif not hadoop_ready:
        hookenv.status_set('waiting',
                           'Waiting for Hadoop')
    elif not database_ready:
        hookenv.status_set('waiting',
                           'Waiting for database')


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


@when('hive.started', 'database.available', 'config.changed.heap')
def reconfigure_hive(database):
    hookenv.status_set('active', 'Configuring Hive')
    hive = Hive(get_dist_config())
    hive.stop()
    hive.configure_hive(database)
    hive.start()
    hookenv.status_set('active', 'Ready')


@when('hive.started')
@when_not_all('hadoop.ready', 'database.available')
def stop_hive():
    hive = Hive(get_dist_config())
    hive.stop()
    hive.close_ports()
    remove_state('hive.started')


@when('hive.installed', 'client.joined')
def client_joined(client):
    dist = get_dist_config()
    port = dist.port('hive')
    client.send_port(port)
    client.set_ready()
