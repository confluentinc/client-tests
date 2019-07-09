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

from kafka_cluster import KafkaCluster
from confluent_kafka import Consumer
import producer
import time
import pytest

@pytest.mark.timeout(5*60)
def test_consumer_cluster_down():
    """ Test that the consumer fully recovers after a cluster failure.
    - Create cluster.
    - Produce N messages.
    - Run consumer, consume N/2 messages.
    - Bring down the entire cluster.
    - Wait at least session.timeout.ms * 2
    - Bring the cluster back up.
    - Wait for remaining messages to be consumed.
    """
    topic = "test"
    msgcnt = 10000
    stop_every = 1000

    cluster = KafkaCluster(broker_cnt=2)
    cluster.start()

    producer.simple_produce_messages(cluster, topic,
                                     partition=0, count=msgcnt,
                                     value_format="{}")

    conf = cluster.client_config()
    conf.update({'group.id': topic,
                 'session.timeout.ms': 6000,
                 'auto.offset.reset': 'earliest',
                 'debug': 'cgrp,broker'})
    c = Consumer(conf)

    c.subscribe([topic])

    next_msgid = 0
    restart_at = None
    while next_msgid < msgcnt:
        m = c.poll(1)
        if m is None:
            if cluster.stopped() and time.time() <= restart_at:
                print("Starting cluster")
                cluster.start()
            continue

        if m.error() is not None:
            print("Consumer error: {}".format(m.error()))
            continue

        msgid = int(m.value())

        if msgid != next_msgid:
            print("Expected msgid {}, got {} for message at offset {}".format(
                next_msgid, msgid, msg.offset()))
            continue

        next_msgid = msgid + 1

        if msgid > 0 and (msgid % stop_every) == 0:
            print("{}/{} messages received: stopping cluster".format(
                next_msgid, msgcnt))
            cluster.stop(cleanup=False)
            if not cluster.cluster.wait_stopped():
                raise RuntimeError("Cluster did not stop in time")
            restart_at = time.time() + 15.0
            time.sleep(10)  # let the client detect the connection loss

    print("{}/{} messages received".format(next_msgid, msgcnt))
    c.close()


if __name__ == '__main__':
    test_consumer_cluster_down()
