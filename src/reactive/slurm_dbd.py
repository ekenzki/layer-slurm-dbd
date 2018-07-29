import socket
import copy
import charms.leadership as leadership
import charms.reactive as reactive
import charms.reactive.flags as flags
import charmhelpers.core.hookenv as hookenv
import charmhelpers.core.host as host
import charms.reactive.relations as relations
import charms.slurm.dbd as dbd
import charmhelpers.fetch as ch_fetch


flags.register_trigger(when='munge.configured',
                       set_flag='slurm-dbd.needs_restart')
flags.register_trigger(when='endpoint.slurm-dbd-ha.joined',
                       clear_flag='slurm-dbd.standalone_startup')


@reactive.when_not('slurmdbd.installed')
def install_slurm():
    hookenv.status_set('maintenance', 'installing slurmdbd packages')
    packages = [dbd.SLURMDBD_PACKAGE]
    ch_fetch.apt_install(packages)
    hookenv.application_version_set(
        ch_fetch.get_upstream_version(dbd.SLURMDBD_PACKAGE))
    flags.set_flag('slurmdbd.installed')


@reactive.when_not('endpoint.slurm-dbd-ha.joined')
@reactive.when_not('slurm-dbd.configured')
def standalone_mode():
    flags.set_flag('slurm-dbd.standalone_startup')


@reactive.when('slurm.installed')
@reactive.when('slurm-dbd.configured')
@reactive.when('munge.configured')
@reactive.when('slurm-dbd.needs_restart')
def handle_munge_change():
    '''
    A trigger sets needs_restart when munge.configured goes from unset to set
    after a change. Need to handle this by restarting slurmctld service.
    '''
    hookenv.status_set('maintenance', 'Munge key changed, restarting service')
    host.service_restart(dbd.SLURMDBD_SERVICE)
    flags.clear_flag('slurm-dbd.needs_restart')


# not an endpoints-based interface at the time of writing
@reactive.when_not('db-mysql.connected')
def missing_db_mysql():
    hookenv.status_set('blocked', 'Missing relation: db-mysql')
    flags.clear_flag('slurm-dbd.configured')
    host.service_stop(dbd.SLURMDBD_SERVICE)


@reactive.when('db-mysql.connected')
@reactive.when_not('db-mysql.available')
def provision_db(db_mysql_endpoint):
    hookenv.status_set('maintenance', 'Requesting a db to be provisioned')
    db_name = hookenv.config('storage_loc')
    username = hookenv.config('storage_user')
    db_mysql_endpoint.configure(database=db_name, username=username)


@reactive.when('leadership.is_leader')
@reactive.when_not('leadership.set.active_dbd')
def set_active_dbd():
    '''Elects an active dbd unit. This is only done once
    until an operator decides to relocate an active dbd
    to a different node via an action or doing a
    juju run --unit <leader-unit> "leader-set active_dbd=''"
    '''
    leadership.leader_set(active_dbd=hookenv.local_unit())


@reactive.when('endpoint.slurm-dbd-ha.joined')
@reactive.when('leadership.set.active_dbd')
def handle_ha(ha_endpoint):
    ''' Provide peer data in order to set up active-backup HA.'''
    peer_data = {'hostname': socket.gethostname()}
    ha_endpoint.provide_peer_data(peer_data)


@reactive.when('slurmdbd.installed')
@reactive.when('munge.configured')
@reactive.when_any('slurm-dbd.standalone_startup',
                   'endpoint.slurm-dbd-ha.changed',
                   'endpoint.slurm-dbd-ha.departed',
                   'config.changed')
@reactive.when('db-mysql.available')
@reactive.when('leadership.set.active_dbd')
def configure_dbd(mysql_endpoint):
    '''A dbd is only configured after leader election is
    performed and a database is believed to be configured'''
    hookenv.status_set('maintenance', 'Configuring slurm-dbd')

    is_active = dbd.is_active_dbd()

    role = dbd.ROLES[is_active]
    peer_role = dbd.ROLES[not is_active]

    dbd_conf = copy.deepcopy(hookenv.config())
    dbd_conf.update({
        'db_hostname': mysql_endpoint.db_host(),
        'db_port': dbd.MYSQL_DB_PORT,
        'db_password': mysql_endpoint.password(),
        'db_name': mysql_endpoint.database(),
        'db_username': mysql_endpoint.username(),
    })

    ha_endpoint = relations.endpoint_from_flag(
        'endpoint.slurm-dbd-ha.joined')
    if ha_endpoint:
        net_details = dbd.add_key_prefix(ha_endpoint.network_details(), role)
        dbd_conf.update(net_details)

        # add prefixed peer data
        peer_data = dbd.add_key_prefix(
            ha_endpoint.peer_data, peer_role)
        dbd_conf.update(peer_data)
    else:
        # if running in standalone mode, just use network-get with HA endpoint
        # name to get an ingress address and a hostname
        net_details = dbd.add_key_prefix(dbd.network_details(), role)
        dbd_conf.update(net_details)
        peer_data = None

    # a dbd service is configurable if it is an active dbd
    # or a backup dbd that knows about an active dbd
    is_configurable = is_active or (not is_active and peer_data)
    if is_configurable:
        hookenv.log('dbd is configurable ({})'.format(role))
        # Setup slurm dirs and config
        dbd.render_slurmdbd_config(context=dbd_conf)
        # Make sure slurmctld is running
        if not host.service_running(dbd.SLURMDBD_SERVICE):
            host.service_start(dbd.SLURMDBD_SERVICE)
        flags.set_flag('slurm-dbd.configured')
        flags.clear_flag('slurm-dbd.standalone_startup')
        host.service_restart(dbd.SLURMDBD_SERVICE)
    else:
        hookenv.log('dbd is NOT configurable ({})'.format(role))
        if not is_active:
            hookenv.status_set('maintenance',
                               'Backup dbd is waiting for peer data')


@reactive.when('db-mysql.connected')
@reactive.when('slurm-dbd.configured')
def dbd_ready(*args):
    hookenv.status_set('active', 'Ready')
