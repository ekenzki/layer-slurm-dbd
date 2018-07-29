import charms.leadership as leadership
import charmhelpers.core.hookenv as hookenv
import socket

from charmhelpers.core.templating import render

SLURMDBD_CONFIG_TEMPLATE = 'slurmdbd.conf'
SLURMDBD_CONFIG_PATH = '/etc/slurm-llnl/slurmdbd.conf'

SLURMDBD_PACKAGE = 'slurmdbd'
SLURMDBD_SERVICE = 'slurmdbd'

MUNGE_SERVICE = 'munge'
MUNGE_KEY_TEMPLATE = 'munge.key'
MUNGE_KEY_PATH = '/etc/munge/munge.key'

MYSQL_DB_PORT = 3306


def render_slurmdbd_config(context):
    render(source=SLURMDBD_CONFIG_TEMPLATE,
           target=SLURMDBD_CONFIG_PATH,
           context=context,
           owner=context.get('slurm_user'),
           group=context.get('slurm_user'),
           perms=0o644)


def network_details():
    # even if there are no relations for that endpoint
    # we can still get an address from a space bound to it
    net_details = hookenv.network_get('slurm-dbd-ha')
    return {
        'hostname': socket.gethostname(),
        'ingress_address': net_details['ingress-addresses'][0]
    }


def add_key_prefix(d, prefix):
    return {'{key_prefix}_{key}'
            .format(key_prefix=prefix, key=k): d[k]
            for k in d.keys()}


def is_active_dbd():
    return leadership.leader_get('active_dbd') == hookenv.local_unit()


ROLES = {True: 'active_dbd', False: 'backup_dbd'}
