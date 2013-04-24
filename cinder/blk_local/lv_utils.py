# vim: tabstop=4 shiftwidth=4 softtabstop=4

# Copyright 2012 OpenStack Foundation.
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
import math
import re

from cinder.volume import utils as vol_utils
from openstack.common import log as logging
from openstack.common import process_utils as putils


LOG = logging.getLogger(__name__)


def get_lsize(name, vg_name=None, suffix='G'):
    lv_ref = get(name, vg_name)
    # TODO(jdg): Check for exception

    (out, err) = putils.execute('lvs', '--noheadings',
                                '-o', 'lv_size',
                                '--units', suffix,
                                lv_ref['LV Name'], run_as_root=True)
    out = out.strip()
    return out

def get(name, vg_name=None):
    '''Inspects system for LV with the given name.

       returns a dict of info representing the lv if exists
       else raises not found

       The reference returned is sucked in directly from
       lvdisplay converting the table output to a dict.
    '''
    if vg_name is None:
        (out, err) = putils.execute('lvs', '--noheadings',
                                    '-o', 'name,vg_name',
                                    run_as_root=True)
        for line in out:
            if name in line:
                parsed_line = line.split()
                if len(parsed_line) > 1:
                    vg_name = parsed_line[1]
                    break
                else:
                    LOG.error(_('Found LV but failed parsing: %s') % line)
                    raise
    if vg_name is None:
        LOG.error(_('Volume Group not found'))
        raise

    lv_path = '%s/%s' % (vg_name, name)
    (out, err) = putils.execute('lvdisplay', lv_path, run_as_root=True)
    formatted_output = re.split(r'\s{2,}', out)

    del formatted_output[-1]
    del formatted_output[0]
    del formatted_output[0]
    return dict(map(None, *[iter(formatted_output)]*2))


def create(name, size, vg_name, mirror_count=0, type='default'):
    cmd = ['lvcreate', '-n', name, vg_name]
    if type == 'thin':
        cmd += ['-T', '-V', vol_utils.int_to_string(size)]
    else:
        cmd += ['-L', vol_utils.int_to_string(size)]

    if mirror_count > 0:
        cmd += ['-m', mirror_count, '--nosync']
        terras = int(size[:-1]) / 1024.0
        if terras >= 1.5:
            rsize = int(2 ** math.ceil(math.log(terras) / math.log(2)))
            # NOTE(vish): Next power of two for region size. See:
            #             http://red.ht/U2BPOD
            cmd += ['-R', str(rsize)]

    (out, err) = putils.execute(*cmd, run_as_root=True)
    return get(name, vg_name)


def create_snapshot(name, source_lv_name, type='default'):
    source_ref = get(name)
    cmd = ['lvcreate', '--name', name, '--snapshot', source_ref['LV Name']]
    if type != 'thin':
        size = source_ref['LV Size']

        cmd += ['-L', size]
    (out, err) = putils.execute(*cmd, run_as_root=True)


def delete(name, vg_name=None):
    lv_ref = get(name, vg_name)
    # if raises/Not Found log warning and move along
    (out, err) = putils.execute('lvremove',
                                lv_ref['LV Name'],
                                run_as_root=True)

def revert(snapshot_name, vg_name=None):
    lv_ref = get(snapshot_name, vg_name)
    (out, err) = putils.execute('lvconvert', '--merge', lv_ref['LV Name'], run_as_root=True)
