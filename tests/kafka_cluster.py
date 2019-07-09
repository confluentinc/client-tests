#!/usr/bin/env python
#
# Copyright 2018 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
from trivup.trivup import Cluster
from trivup.apps.ZookeeperApp import ZookeeperApp
from trivup.apps.KafkaBrokerApp import KafkaBrokerApp


class KafkaCluster(object):
    def __init__(self, broker_cnt=3, kafka_version='2.0.0'):
        super(KafkaCluster, self).__init__()

        self.cluster = Cluster('KafkaCluster',
                               root_path=os.environ.get('TRIVUP_ROOT', 'tmp'),
                               debug=True)

        self.apps = dict()

        # Create a single ZK for the cluster
        ZookeeperApp(self.cluster)

        # Create broker_cnt brokers
        brokerconf = {'replication_factor': min(3, int(broker_cnt)),
                      'num_partitions': 4,
                      'version': kafka_version}

        self.brokers = dict()
        for n in range(0, broker_cnt):
            broker = KafkaBrokerApp(self.cluster, brokerconf)
            self.brokers[broker.appid] = broker

        # Get bootstrap server list
        security_protocol = 'PLAINTEXT'
        all_listeners = (','.join(self.cluster.get_all(
            'listeners', '', KafkaBrokerApp))).split(',')
        bootstrap_servers = ','.join([x for x in all_listeners
                                      if x.startswith(security_protocol)])

        # Create client base configuration
        self._client_config = {
            'bootstrap.servers': bootstrap_servers,
            'broker.address.family': 'v4'
        }

        self.cluster.deploy()

    def client_config(self):
        return self._client_config.copy()

    def start(self):
        self.cluster.start()
        if not self.cluster.wait_operational(30):
            self.cluster.stop(force=True)
            raise Exception("Cluster {} did not go operational, "
                            "see logs in {}".format(
                                self.cluster.name,
                                self.cluster.instance_path()))

    def stop(self, cleanup=True):
        self.cluster.stop()
        if cleanup:
            self.cluster.cleanup(keeptypes=['log'])

    def stopped(self):
        """ Returns True when all components of the cluster are stopped """
        return len([x for x in self.cluster.apps if x.status() == 'stopped']) == len(self.cluster.apps)

    def stop_broker(self, broker_id):
        broker = self.brokers.get(broker_id, None)
        if broker is None:
            raise LookupError("Unknown broker id {}".format(broker_id))
        broker.stop()

    def start_broker(self, broker_id):
        broker = self.brokers.get(broker_id, None)
        if broker is None:
            raise LookupError("Unknown broker id {}".format(broker_id))
        broker.start()
