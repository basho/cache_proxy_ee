#!/usr/bin/env python
#coding: utf-8
#file   : server_modules.py
#author : ning
#date   : 2014-02-24 13:00:28

import os
import sys

from utils import *
import conf
import subprocess
from riak import RiakError

# NOTE: did not derive from ServerBase as the Riak cluster is easier to manage
# as duck equivalent instead of actually inheritence chain equivalence.

class RiakCluster:
    # node_name_ports is an array of tuples of (node_name, port) where port is
    # the protobuf port from which all other ports ascend, ie http is pb_port
    # + 10000 .
    def __init__(self, node_name_ports):
        self.args = {
                'name'             : 'riak',
                'node_name_ports'  : node_name_ports,
                }
        self._shutdowned_nodes = []

    def __str__(self):
        return TT('[$name:$node_name_ports]', self.args)

    def deploy(self):
        logging.info('deploy %s' % self)
        for (node_name, port) in self.node_name_ports():
            self._run(TT('./_binaries/create_riak_devrel_from_tarball.sh $node_name $port', {'node_name': node_name, 'port': port}))

    def start(self):
        if self._alive():
            logging.warn('%s already start' % (self))
            return

        t1 = time.time()
        max_wait = 60
        while not self._alive():
            if self._start():
                break
            if time.time() - t1 > max_wait:
                break
        t2 = time.time()
        logging.info('%s start ok in %.2f seconds' % (self, t2 - t1))
        self._ensure_string_dt()
        self._ensure_set_dt()
        logging.info('set bucket-type exists, observe, add, remove away!')

        if len(self.node_name_ports()) > 1:
            self._cluster_command('./_binaries/create_riak_cluster.sh', 3)

    def _start(self):
        ret = self._cluster_command('./_binaries/service_riak_nodes.sh start', 3)
        return 0 == ret

    def stop(self):
        if not self._alive():
            logging.warn('%s already stop' % (self))
            return

        t1 = time.time()
        max_wait = 60
        while self._alive():
            if self._stop():
                break
            if time.time() - t1 > max_wait:
                break
        t2 = time.time()
        logging.info('%s stop ok in %.2f seconds' %(self, t2 - t1))

    def _stop(self):
        if len(self.node_name_ports()) > 1:
            try:
                self._cluster_command('./_binaries/teardown_riak_cluster.sh', 3)
            except subprocess.CalledProcessError:
                pass
        ret = self._cluster_command('./_binaries/service_riak_nodes.sh stop', 3)
        return 0 == ret

    def _cluster_command(self, command_script, retries = 0, retry_delay = 0.1):
        return self._nodes_command(self.node_names(), command_script, retries, retry_delay)

    def _nodes_command(self, node_names, command_script, retries = 0, retry_delay = 0.1):
        cmd_args = {
                'command_script': command_script,
                'node_names': ' '.join(node_names)
                }

        retries += 1
        while retries > 0:
            try:
                ret = self._run(TT('$command_script $node_names', cmd_args))
                retries = 0
                ei = None
            except subprocess.CalledProcessError:
                retries -= 1
                ei = sys.exc_info()
                time.sleep(retry_delay)

        if ei != None:
            raise ei[1], None, ei[2]
        return ret

    def node_name_ports(self):
        return self.args['node_name_ports']

    def node_names(self):
        return map(lambda (nn, p): nn, self.node_name_ports())

    def pid(self):
        logging.warn('pid: not implemented')

    def status(self):
        logging.warn('status: not implemented')

    def _alive(self):
        try:
            return self.__alive()
        except: # TimeoutError:
            return False

    def __alive(self):
       ret = self._cluster_command('./_binaries/service_riak_nodes.sh ping', 3)
       return 0 == ret

    def _run(self, raw_cmd):
        logging.debug('running: %s' % raw_cmd)
        ret = 1
        outfile = getenv('T_RIAK_TEST_LOG', os.devnull)
        with open(outfile, 'a') as devnull:
            ret = subprocess.call(raw_cmd.split(), stdout=devnull, stderr=subprocess.STDOUT)
        logging.debug('[%d] %s' % (ret, raw_cmd))
        return ret

    def clean(self):
        pass

    def base_dir(self):
        return '/tmp/r'

    def host(self):
        return '127.0.0.1'

    def port(self):
        return self._pb_port(self.node_names()[0])

    def port_from_node_name(self, name):
        return self._pb_port(name)

    def _devrel_path(self, node_name):
        return '%s/riak_devrel_%s' % (self.base_dir(), node_name)

    def _pb_port(self, node_name):
        # could easily read the arg, but checking the configured value is better
        riak_conf_path = '%s/etc/riak.conf' % (self._devrel_path(node_name))
        for conf_line in open(riak_conf_path, 'r'):
            if 0 <= conf_line.find('listener.protobuf.internal'):
                return int(conf_line.split(':')[-1])

        return -1

    def shutdown(self, node_names):
        self._shutdowned_nodes.extend(node_names);
        ret = self._nodes_command(node_names, './_binaries/service_riak_nodes.sh stop', 3)
        return 0 == ret

    def restore(self):
        if len(self._shutdowned_nodes) == 0:
            return True
        ret = self._nodes_command(self._shutdowned_nodes, './_binaries/service_riak_nodes.sh start', 3)
        self._shutdowned_nodes = []
        if len(self.node_name_ports()) > 1:
            self._cluster_command('./_binaries/create_riak_cluster.sh', 3)
        return 0 == ret

    def _ensure_string_dt(self):
        node_name = self.node_names()[0]
        self.__alive() or self.start()
        bucket_type_options = { \
            'devrel_path': self._devrel_path(node_name) \
            ,'bucket_type': "strings" \
            ,'bucket_type_props': '{"props":{}}' \
        }

        if 0 == self._run(TT('$devrel_path/bin/riak-admin bucket-type status $bucket_type', \
                bucket_type_options)):
                return
                
        if 0 != self._run(TT('$devrel_path/bin/riak-admin bucket-type create $bucket_type $bucket_type_props', \
                bucket_type_options)):
                raise RiakError('Unable to create set bucket_type')

        if 0 != self._run(TT('$devrel_path/bin/riak-admin bucket-type activate $bucket_type', \
                bucket_type_options)):
                raise RiakError('Unable to activate set bucket_type')

    def _ensure_dt_bucket_type(self, bucket_type_name, datatype):
        node_name = self.node_names()[0]
        self.__alive() or self.start()
        bucket_type_options = { \
            'devrel_path': self._devrel_path(node_name) \
            ,'bucket_type': bucket_type_name \
            ,'bucket_type_props': '{"props":{"datatype":"' + datatype + '"}}' \
        }

        if 0 == self._run(TT('$devrel_path/bin/riak-admin bucket-type status $bucket_type', \
                bucket_type_options)):
                return
                
        if 0 != self._run(TT('$devrel_path/bin/riak-admin bucket-type create $bucket_type $bucket_type_props', \
                bucket_type_options)):
                raise RiakError('Unable to create set bucket_type')

        if 0 != self._run(TT('$devrel_path/bin/riak-admin bucket-type activate $bucket_type', \
                bucket_type_options)):
                raise RiakError('Unable to activate set bucket_type')

    def _ensure_set_dt(self):
        # TODO: set bucket-type name should be arbitrary, but it is hardcoded
        # in cache proxy. we may be okay with that, but follow up
        self._ensure_dt_bucket_type('sets', 'set')
