#!/usr/bin/env python
#
# Copyright 2019 Confluent Inc.
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

from trivup.trivup import Cluster, TcpPortAllocator
from trivup.apps.ZookeeperApp import ZookeeperApp
from trivup.apps.KafkaBrokerApp import KafkaBrokerApp
from trivup.apps.KerberosKdcApp import KerberosKdcApp

from confluent_kafka import Producer

import time
import os


def test_kerberos_cross_realm():
    """ Test Kerberos cross-realm trusts """
    topic = "test"

    cluster = Cluster('KafkaCluster',
                      root_path=os.environ.get('TRIVUP_ROOT', 'tmp'),
                      debug=True)

    ZookeeperApp(cluster)

    #
    # Create KDCs for each realm.
    # First realm will be the default / broker realm.
    #
    realm_cnt = 2
    realms = ["REALM{}.COM".format(x + 1) for x in range(0, realm_cnt)]

    # Pre-Allocate ports for the KDCs so they can reference eachother
    # in the krb5.conf configuration.
    kdc_ports = {x: TcpPortAllocator(cluster).next("dummy") for x in realms}

    # Set up realm=kdc:port cross-realm mappings
    cross_realms = ",".join(["{}={}:{}".format(x, cluster.get_node().name, kdc_ports[x]) for x in realms])

    kdcs = dict()
    for realm in realms:
        kdc = KerberosKdcApp(cluster, realm,
                             conf={'port': kdc_ports[realm],
                                   'cross_realms': cross_realms,
                                   'renew_lifetime': '30',
                                   'ticket_lifetime': '120'})
        kdc.start()
        kdcs[realm] = kdc

    broker_realm = realms[0]
    client_realm = realms[1]
    broker_kdc = kdcs[broker_realm]
    client_kdc = kdcs[client_realm]

    # Create broker_cnt brokers
    broker_cnt = 4
    brokerconf = {'replication_factor': min(3, int(broker_cnt)),
                  'num_partitions': broker_cnt * 2,
                  'version': '2.2.0',
                  'sasl_mechanisms': 'GSSAPI',
                  'realm': broker_realm,
                  'conf': ['connections.max.idle.ms=60000']}

    brokers = dict()
    for n in range(0, broker_cnt):
        broker = KafkaBrokerApp(cluster, brokerconf)
        brokers[broker.appid] = broker

    # Get bootstrap server list
    security_protocol = 'SASL_PLAINTEXT'
    all_listeners = (','.join(cluster.get_all(
        'listeners', '', KafkaBrokerApp))).split(',')
    bootstrap_servers = ','.join([x for x in all_listeners
                                  if x.startswith(security_protocol)])

    assert len(bootstrap_servers) > 0, "no bootstrap servers"

    print("## Deploying cluster")
    cluster.deploy()
    print("## Starting cluster")
    cluster.start(timeout=30)

    # Add cross-realm TGTs
    for realm in realms:
        for crealm in [x for x in realms if x != realm]:
            kdcs[realm].execute('kadmin.local -d "{}" -q "addprinc -requires_preauth -pw password krbtgt/{}@{}"'.format(kdcs[realm].conf.get('dbpath'), crealm, realm)).wait()
            kdcs[realm].execute('kadmin.local -d "{}" -q "addprinc -requires_preauth -pw password krbtgt/{}@{}"'.format(kdcs[realm].conf.get('dbpath'), realm, crealm)).wait()

    # Create client base configuration
    client_config = {
        'bootstrap.servers': bootstrap_servers,
        'enable.sparse.connections': False,
        'broker.address.family': 'v4',
        'sasl.mechanisms': 'GSSAPI',
        'security.protocol': security_protocol,
        'debug': 'broker,security'
    }

    os.environ['KRB5CCNAME'] = client_kdc.mkpath('krb5cc')
    os.environ['KRB5_CONFIG'] = client_kdc.conf['krb5_conf']
    os.environ['KRB5_KDC_PROFILE'] = client_kdc.conf['kdc_conf']
    principal,keytab = client_kdc.add_principal("admin")

    client_config['sasl.kerberos.keytab'] = keytab
    client_config['sasl.kerberos.principal'] = principal.split('@')[0]
    client_config['sasl.kerberos.min.time.before.relogin'] = 120*1000*3

    print(client_config)

    print("bootstraps: {}".format(client_config['bootstrap.servers']))
    p = Producer(client_config)

    time.sleep(10)
    for n in range(1, 100):
        p.produce(topic, "msg #{}".format(n))

        p.poll(1.0)

    p.flush(1.0)

    print("####### {} messages remaining\n\n\n".format(len(p)))

    start = time.time()
    end = start + (90*60)
    until = start + (12*60)
    while time.time() < end:
        now = time.time()
        if until < now:
            print("### Producing 2 messages")
            for n in range(1, 2):
                p.produce(topic, "msg #{}".format(n))
            until = now + (12*60)

        p.poll(1.0)

    del p

    cluster.stop()


if __name__ == '__main__':
    test_kerberos_cross_realm()
