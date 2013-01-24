# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright (c) 2012 OpenStack, LLC.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

"""Volume-related Utilities and helpers."""

import paramiko
import random

from eventlet import greenthread

from cinder import exception
from cinder import flags
from cinder.openstack.common import cfg
from cinder.openstack.common import log as logging
from cinder.openstack.common.notifier import api as notifier_api
from cinder.openstack.common import timeutils
from cinder import utils


san_options = [
    cfg.BoolOpt('san_thin_provision',
                default=True,
                help='Use thin provisioning for SAN volumes?'),
    cfg.StrOpt('san_ip',
               default='',
               help='IP address of SAN controller'),
    cfg.StrOpt('san_login',
               default='admin',
               help='Username for SAN controller'),
    cfg.StrOpt('san_password',
               default='',
               help='Password for SAN controller'),
    cfg.StrOpt('san_private_key',
               default='',
               help='Filename of private key to use for SSH authentication'),
    cfg.StrOpt('san_clustername',
               default='',
               help='Cluster name to use for creating volumes'),
    cfg.IntOpt('san_ssh_port',
               default=22,
               help='SSH port to use with SAN'),
    cfg.BoolOpt('san_is_local',
                default=False,
                help='Execute commands locally instead of over SSH; '
                     'use if the volume service is running on the SAN device'),
    cfg.IntOpt('ssh_conn_timeout',
               default=30,
               help="SSH connection timeout in seconds"),
    cfg.IntOpt('ssh_min_pool_conn',
               default=1,
               help='Minimum ssh connections in the pool'),
    cfg.IntOpt('ssh_max_pool_conn',
               default=5,
               help='Maximum ssh connections in the pool'), ]

FLAGS = flags.FLAGS
FLAGS.register_opts(san_options)

LOG = logging.getLogger(__name__)


def notify_usage_exists(context, volume_ref, current_period=False):
    """ Generates 'exists' notification for a volume for usage auditing
        purposes.

        Generates usage for last completed period, unless 'current_period'
        is True."""
    begin, end = utils.last_completed_audit_period()
    if current_period:
        audit_start = end
        audit_end = timeutils.utcnow()
    else:
        audit_start = begin
        audit_end = end

    extra_usage_info = dict(audit_period_beginning=str(audit_start),
                            audit_period_ending=str(audit_end))

    notify_about_volume_usage(context, volume_ref,
                              'exists', extra_usage_info=extra_usage_info)


def _usage_from_volume(context, volume_ref, **kw):
    def null_safe_str(s):
        return str(s) if s else ''

    usage_info = dict(tenant_id=volume_ref['project_id'],
                      user_id=volume_ref['user_id'],
                      volume_id=volume_ref['id'],
                      volume_type=volume_ref['volume_type_id'],
                      display_name=volume_ref['display_name'],
                      launched_at=null_safe_str(volume_ref['launched_at']),
                      created_at=null_safe_str(volume_ref['created_at']),
                      status=volume_ref['status'],
                      snapshot_id=volume_ref['snapshot_id'],
                      size=volume_ref['size'])

    usage_info.update(kw)
    return usage_info


def notify_about_volume_usage(context, volume, event_suffix,
                              extra_usage_info=None, host=None):
    if not host:
        host = FLAGS.host

    if not extra_usage_info:
        extra_usage_info = {}

    usage_info = _usage_from_volume(context, volume, **extra_usage_info)

    notifier_api.notify(context, 'volume.%s' % host,
                        'volume.%s' % event_suffix,
                        notifier_api.INFO, usage_info)

def build_iscsi_target_name(self, volume):
    return "%s%s" % (FLAGS.iscsi_target_prefix, volume['name'])

def san_execute(self, *cmd, **kwargs):
    if self.san_is_local:
        return utils.execute(*cmd, **kwargs)
    else:
        check_exit_code = kwargs.pop('check_exit_code', None)
        command = ' '.join(cmd)
        return self._run_ssh(command, check_exit_code)

def _run_ssh(self, command, check_exit_code=True, attempts=1):
    if not self.sshpool:
        self.sshpool = utils.SSHPool(FLAGS.san_ip,
                                     FLAGS.san_ssh_port,
                                     FLAGS.ssh_conn_timeout,
                                     FLAGS.san_login,
                                     password=FLAGS.san_password,
                                     privatekey=FLAGS.san_private_key,
                                     min_size=FLAGS.ssh_min_pool_conn,
                                     max_size=FLAGS.ssh_max_pool_conn)
    try:
        total_attempts = attempts
        with self.sshpool.item() as ssh:
            while attempts > 0:
                attempts -= 1
                try:
                    return utils.ssh_execute(
                        ssh,
                        command,
                        check_exit_code=check_exit_code)
                except Exception as e:
                    LOG.error(e)
                    greenthread.sleep(random.randint(20, 500) / 100.0)
            raise paramiko.SSHException(_("SSH Command failed after "
                                          "'%(total_attempts)r' attempts"
                                          ": '%(command)s'"), locals())
    except Exception as e:
        LOG.error(_("Error running ssh command: %s") % command)
        raise e

def check_for_san_setup_error():
    if not FLAGS.san_is_local:
        if not (FLAGS.san_password or FLAGS.san_private_key):
            raise exception.InvalidInput(
                reason=_('Specify san_password or san_private_key'))
    if not FLAGS.san_ip:
        raise exception.InvalidInput(reason=_('san_ip must be set'))
