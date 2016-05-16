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

    def __str__(self):
        return TT('[$name:$node_name_ports]', self.args)

    def deploy(self):
        logging.info('deploy %s' % self)
        for (node_name, port) in self.node_name_ports():
            self._run(TT('./_binaries/create_riak_devrel_from_tarball.sh $node_name $port', {'node_name': node_name, 'port': port}))

        if not self.start():
            raise RiakError('Unable to start Riak nodes during deploy')

        if len(self.node_name_ports()) > 1:
            self._cluster_command('./_binaries/create_riak_cluster.sh', 3)

    def start(self):
        t1 = time.time()
        max_wait = getenv('T_RIAK_START_MAX_WAIT', 60, lambda x: int(x))
        is_alive = 1 == 0
        while not is_alive:
            is_alive = self._start()
            if not is_alive:
                if time.time() - t1 > max_wait:
                    break
                time.sleep(1)
        t2 = time.time()
        logging.info('%s start ok in %.2f seconds' % (self, t2 - t1))
        return is_alive

    def _start(self):
        ret = self._cluster_command('./_binaries/service_riak_nodes.sh start', 3)
        return 0 == ret

    def stop(self):
        t1 = time.time()
        max_wait = 60
        while self._alive():
            if self._stop():
                break
            if time.time() - t1 > max_wait:
                break
            time.sleep(1)
        t2 = time.time()
        logging.info('%s stop ok in %.2f seconds' %(self, t2 - t1))

    def _stop(self):
        ret = self._cluster_command('./_binaries/service_riak_nodes.sh stop', 3)
        return 0 == ret

    def _cluster_command(self, command_script, retries = 0, retry_delay = 1):
        return self._nodes_command(self.node_names(), command_script, retries, retry_delay)

    def _nodes_command(self, node_names, command_script, retries = 0, retry_delay = 1):
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

    def _log_riak_line(self, line):
        outfile_path = getenv('T_RIAK_TEST_LOG', os.devnull)
        with open(outfile_path, 'a') as outfile:
            outfile.write(line + '\n')

    def _run(self, raw_cmd):
        logging.debug('running: %s' % raw_cmd)
        ret = 1
        outfile_path = getenv('T_RIAK_TEST_LOG', os.devnull)
        self._log_riak_line("_running: %s" % raw_cmd)
        with open(outfile_path, 'a') as outfile:
            ret = subprocess.call(raw_cmd.split(), stdout=outfile, stderr=subprocess.STDOUT)
        logging.debug('[%d] %s' % (ret, raw_cmd))
        self._log_riak_line("_ran [ %d ]: %s" % (ret, raw_cmd))
        return ret

    def clean(self):
        self._teardown_cluster()

    def _teardown_cluster(self):
        if len(self.node_name_ports()) > 1:
            try:
                self._cluster_command('./_binaries/teardown_riak_cluster.sh', 3)
            except subprocess.CalledProcessError:
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
        ret = self._nodes_command(node_names, './_binaries/service_riak_nodes.sh stop', 3)
        if 0 == ret:
            self._teardown_cluster()
        return 0 == ret

    def restore(self):
        return self.start()

    def ensure_string_dt(self):
        node_name = self.node_names()[0]
        self.__alive() or self.start()
        bucket_type_name = 'strings'
        bucket_type_options = { \
            'devrel_path': self._devrel_path(node_name) \
            ,'bucket_type': bucket_type_name \
            ,'bucket_type_props': '{"props":{}}' \
        }

        if 0 == self._run(TT('$devrel_path/bin/riak-admin bucket-type status $bucket_type', \
                bucket_type_options)):
                return
                
        if 0 != self._run(TT('$devrel_path/bin/riak-admin bucket-type create $bucket_type $bucket_type_props', \
                bucket_type_options)):
                raise RiakError('Unable to create {0} bucket_type'.format(bucket_type_name))

        if 0 != self._run(TT('$devrel_path/bin/riak-admin bucket-type activate $bucket_type', \
                bucket_type_options)):
                raise RiakError('Unable to activate {0} bucket_type'.format(bucket_type_name))


    def ensure_dt_bucket_type(self, bucket_type_name, datatype):
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
                raise RiakError('Unable to create {0} bucket_type'.format(bucket_type_name))

        if 0 != self._run(TT('$devrel_path/bin/riak-admin bucket-type activate $bucket_type', \
                bucket_type_options)):
                raise RiakError('Unable to activate {0} bucket_type'.format(bucket_type_name))

    def ensure_set_dt(self):
        # cache proxy supports arbitrary datatype name just like bucket, but
        # using 'sets' to reduce bucket type setup
        self.ensure_dt_bucket_type('sets', 'set')
