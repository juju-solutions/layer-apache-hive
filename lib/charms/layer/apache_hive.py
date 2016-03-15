import time
import os
import signal
from subprocess import Popen

import jujuresources
from charmhelpers.core import hookenv
from charmhelpers.core import unitdata
from jujubigdata import utils


class Hive(object):
    """
    This class manages the deployment steps of Hive.

    :param DistConfig dist_config: The configuration container object needed.
    """

    HIVE_VERSION = {'x86_64': '1.0.0', 'ppc64le': '0.13.0'}

    def __init__(self, dist_config):
        self.dist_config = dist_config
        self.cpu_arch = utils.cpu_arch()
        self.resources = {
            'hive': 'hive-%s' % self.cpu_arch,
        }
        self.verify_resources = utils.verify_resources(*self.resources.values())

    def is_installed(self):
        return unitdata.kv().get('hive.installed')

    def install(self, force=False):
        '''
        Create the users and directories. This method is to be called only once.

        :param bool force: Force the installation execution even if this is not the first installation attempt.
        '''
        if not force and self.is_installed():
            return
        jujuresources.install(self.resources['hive'],
                              destination=self.dist_config.path('hive'),
                              skip_top_level=True)
        self.dist_config.add_users()
        self.dist_config.add_dirs()
        self.dist_config.add_packages()

        unitdata.kv().set('hive.installed', True)
        unitdata.kv().flush(True)

    def setup_hive_config(self):
        '''
        copy the default configuration files to hive_conf property
        defined in dist.yaml
        '''
        default_conf = self.dist_config.path('hive') / 'conf'
        hive_conf = self.dist_config.path('hive_conf')
        hive_conf.rmtree_p()
        default_conf.copytree(hive_conf)

        # Configure immutable bits
        hive_bin = self.dist_config.path('hive') / 'bin'
        with utils.environment_edit_in_place('/etc/environment') as env:
            if hive_bin not in env['PATH']:
                env['PATH'] = ':'.join([env['PATH'], hive_bin])
            env['HIVE_CONF_DIR'] = self.dist_config.path('hive_conf')

        hive_env = self.dist_config.path('hive_conf') / 'hive-env.sh'
        if not hive_env.exists():
            (self.dist_config.path('hive_conf') / 'hive-env.sh.template').copy(hive_env)

        hive_site = self.dist_config.path('hive_conf') / 'hive-site.xml'
        if not hive_site.exists():
            (self.dist_config.path('hive_conf') / 'hive-default.xml.template').copy(hive_site)
        with utils.xmlpropmap_edit_in_place(hive_site) as props:
            # TODO (kwm): we should be able to export java.io.tmpdir so these 4 arent needed
            props['hive.exec.local.scratchdir'] = "/tmp/hive"
            props['hive.downloaded.resources.dir'] = "/tmp/hive_resources"
            props['hive.querylog.location'] = "/tmp/hive"
            props['hive.server2.logging.operation.log.location'] = "/tmp/hive"
            ####

        # create hdfs storage space
        utils.run_as('hive', 'hdfs', 'dfs', '-mkdir', '-p', '/user/hive/warehouse')

    # called during config-changed events
    def configure_hive(self, mysql):
        config = hookenv.config()
        hive_site = self.dist_config.path('hive_conf') / 'hive-site.xml'
        with utils.xmlpropmap_edit_in_place(hive_site) as props:
            props['javax.jdo.option.ConnectionURL'] = "jdbc:mysql://{}:{}/{}".format(
                mysql.host(), mysql.port(), mysql.database()
            )
            props['javax.jdo.option.ConnectionUserName'] = mysql.user()
            props['javax.jdo.option.ConnectionPassword'] = mysql.password()
            props['javax.jdo.option.ConnectionDriverName'] = "com.mysql.jdbc.Driver"
            props['hive.hwi.war.file'] = "lib/hive-hwi-%s.jar" % self.HIVE_VERSION[self.cpu_arch]

        hive_env = self.dist_config.path('hive_conf') / 'hive-env.sh'
        utils.re_edit_in_place(hive_env, {
            r'.*export HADOOP_HEAPSIZE *=.*': 'export HADOOP_HEAPSIZE=%s' % config['heap'],
            r'.*export HIVE_AUX_JARS_PATH *=.*': 'export HIVE_AUX_JARS_PATH=/usr/share/java/mysql-connector-java.jar',
        })

        # Now that we have db connection info, init our schema (only once)
        if not unitdata.kv().get('hive.schema.initialized'):
            utils.run_as('hive', 'schematool', '-initSchema', '-dbType', 'mysql')
            unitdata.kv().set('hive.schema.initialized', True)

    def new_db_connection(self):
        if unitdata.kv().get('hive.schema.initialized'):
            unitdata.kv().set('hive.schema.initialized', False)


    def run_bg(self, user, command, *args):
        """
        Run a Hive command as the `hive` user in the background.

        :param str command: Command to run
        :param list args: Additional args to pass to the command
        """
        parts = [command] + list(args)
        quoted = ' '.join("'%s'" % p for p in parts)
        e = utils.read_etc_env()
        Popen(['su', user, '-c', quoted], env=e)

    def open_ports(self):
        for port in self.dist_config.exposed_ports('hive'):
            hookenv.open_port(port)

    def close_ports(self):
        for port in self.dist_config.exposed_ports('hive'):
            hookenv.close_port(port)

    def start(self):
        self.stop()
        self.run_bg(
            'hive', 'hive',
            '--config', self.dist_config.path('hive_conf'),
            '--service', 'hiveserver2')
        time.sleep(5)

    def stop(self):
        hive_pids = utils.jps('HiveServer2')
        for pid in hive_pids:
            os.kill(int(pid), signal.SIGTERM)

    def cleanup(self):
        self.dist_config.remove_users()
        self.dist_config.remove_dirs()
