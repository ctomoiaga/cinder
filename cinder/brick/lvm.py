# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2013 OpenStack Foundation.
# All Rights Reserved.
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

"""
LVM class for performing LVM operations.
"""

import math

from itertools import izip

from cinder.openstack.common.gettextutils import _
from cinder.openstack.common import log as logging
from cinder.openstack.common import processutils as putils

LOG = logging.getLogger(__name__)


class VolumeGroupNotFound(Exception):
    def __init__(self, vg_name):
        message = ('Unable to find Volume Group: %s' % vg_name)
        super(VolumeGroupNotFound, self).__init__(message)

class LVM():
    """LVM object to enable various LVM related operations."""

    def __init__(self, vg_name, create_vg=False,
                 physical_volumes=None, **kwargs):
        """Initialize the LVM object.

        The LVM object is based on an LVM VolumeGroup, one instantiation
        for each VolumeGroup you have/use.

        :param vg_name: Name of existing VG or VG to create
        :param create_vg: Indicates the VG doesn't exist
                          and we want to creat it
        :param physical_volumes: List of PV's to build VG on

        """
        self.vg_name = vg_name
        self.pv_list = []
        self.lv_list = []
        self.vg_size = 0
        self.vg_available_space = 0
        self.vg_lv_count = 0
        self.vg_uuid = None

        if create_vg and physical_volumes is not None:
            self.pv_list = physical_volumes
            try:
                self._create_vg(physical_volumes)
            except:
                LOG.error(_('Failed initialization create Volume Group.'))
                raise

        if self._vg_exists() is False:
            LOG.error(_('Unable to locate Volume Group %s') % vg_name)
            raise VolumeGroupNotFound(vg_name=vg_name)

    def _vg_exists(self):
        exists = True
        cmd = ['vgs', '--noheadings', '-o', 'name']
        (out, err) = putils.execute(*cmd, root_helper='sudo', run_as_root=True)
        volume_groups = out.split()
        if self.vg_name not in volume_groups:
            exists = False
        return exists

    def _create_vg(self, pv_list):
        cmd = ['vgcreate', self.vg_name, ','.join(pv_list)]
        (out, err) = putils.execute(*cmd, root_helper='sudo', run_as_root=True)

    def _get_vg_uuid(self):
        (out, err) = putils.execute('vgs', '-o uuid', self.vg_name)
        return out.split()

    @staticmethod
    def get_all_volumes(vg_name=None):
        """Static method to get all LV's on a system.

        :param vg_name: optional, gathers info for only the specified VG
        :returns: List of Dictionaries with LV info

        """
        cmd = ['lvs', '--noheadings', '-o', 'vg_name,name,size']
        if vg_name is not None:
            cmd += [vg_name]

        (out, err) = putils.execute(*cmd, root_helper='sudo', run_as_root=True)

        volumes = out.split()
        lv_list = []
        for vg, name, size in izip(*[iter(volumes)] * 3):
            lv_list.append({"vg": vg, "name": name, "size": size})
        return lv_list

    def get_volumes(self):
        """Get all LV's associated with this instantiation (VG).

        :returns: List of Dictionaries with LV info

        """
        self.lv_list = self.get_all_volumes(self.vg_name)
        return self.lv_list

    def get_volume(self, name):
        ref_list = self.get_volumes()
        for r in ref_list:
            if r['name'] == name:
                return r

    @staticmethod
    def get_all_physical_volumes(vg_name=None):
        """Static method to get all PV's on a system.

        :param vg_name: optional, gathers info for only the specified VG
        :returns: List of Dictionaries with PV info

        """
        cmd = ['pvs', '--noheadings',
               '-o', 'vg_name,name,size,free',
               '--separator', ':']
        if vg_name is not None:
            cmd += [vg_name]

        (out, err) = putils.execute(*cmd, root_helper='sudo', run_as_root=True)

        pvs = out.split()
        pv_list = []
        for pv in pvs:
            fields = pv.split(':')
            pv_list.append({'vg': fields[0],
                            'name': fields[1],
                            'size': fields[2],
                            'available': fields[3]})

        return pv_list

    def get_physical_volumes(self):
        """Get all PV's associated with this instantiation (VG).

        :returns: List of Dictionaries with PV info

        """
        self.pv_list = self.get_all_physical_volumes(self.vg_name)
        return self.pv_list

    @staticmethod
    def get_all_volume_groups(vg_name=None):
        """Static method to get all VG's on a system.

        :param vg_name: optional, gathers info for only the specified VG
        :returns: List of Dictionaries with VG info

        """
        cmd = ['vgs', '--noheadings',
               '-o', 'name,size,free,lv_count,uuid',
               '--separator', ':']
        if vg_name is not None:
            cmd += [vg_name]

        (out, err) = putils.execute(*cmd, root_helper='sudo', run_as_root=True)

        vgs = out.split()
        vg_list = []
        for vg in vgs:
            fields = vg.split(':')
            vg_list.append({'name': fields[0],
                            'size': fields[1],
                            'available': fields[2],
                            'lv_count': fields[3],
                            'uuid': fields[4]})

        return vg_list

    def update_volume_group_info(self):
        """Update VG info for this instantiation.

        Used to update member fields of object and
        provide a dict of info for caller.

        :returns: Dictionaries of VG info

        """
        vg_list = self.get_all_physical_volumes(self.vg_name)

        if len(vg_list) > 1:
            LOG.error(_('Something is seriously jacked up....'))

        self.vg_size = vg_list[0]['size']
        self.vg_available_space = vg_list[0]['available']
        self.vg_lv_count = vg_list[0]['lv_count']
        self.vg_uuid = vg_list[0]['uuid']

        return vg_list[0]

    def create_volume(self, name, size, type='default', mirror_count=0):
        #TODO(jdg): Run the size through check/conversion
        cmd = ['lvcreate', '-n', name, self.vg_name]
        if type == 'thin':
            cmd += ['-T', '-V', size]
        else:
            cmd += ['-L', size]

        if mirror_count > 0:
            cmd += ['-m', mirror_count, '--nosync']
            terras = int(size[:-1]) / 1024.0
            if terras >= 1.5:
                rsize = int(2 ** math.ceil(math.log(terras) / math.log(2)))
                # NOTE(vish): Next power of two for region size. See:
                #             http://red.ht/U2BPOD
                cmd += ['-R', str(rsize)]

        (out, err) = putils.execute(*cmd,
                                    root_helper='sudo',
                                    run_as_root=True)
        LOG.debug("output was: %s" % out)

    def create_lv_snapshot(self, name, source_lv_name, type='default'):
        source_lvref = self.get_volume(source_lv_name)
        if source_lvref is None:
            LOG.error(_("FAIL"))
            return False
        cmd = ['lvcreate', '--name', name,
               '--snapshot',  '%s/%s' % (self.vg_name, source_lv_name)]
        if type != 'thin':
            size = source_lvref['size']
            cmd += ['-L', size]

        (out, err) = putils.execute(*cmd,
                                    root_helper='sudo',
                                    run_as_root=True)

    def delete(self, name):
        (out, err) = putils.execute('lvremove',
                                    '-f',
                                    '%s/%s' % (self.vg_name, name),
                                    root_helper='sudo', run_as_root=True)

    def revert(self, snapshot_name):
        (out, err) = putils.execute('lvconvert', '--merge',
                                    snapshot_name, root_helper='sudo',
                                    run_as_root=True)
