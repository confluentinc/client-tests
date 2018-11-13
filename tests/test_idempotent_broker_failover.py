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
from producer import RateProducer
from scheduler import ThreadScheduler


def test_broker_failover():
    topic = "test"

    cluster = KafkaCluster(broker_cnt=2)
    cluster.start()

    sched = ThreadScheduler(2)  # one for sender, one for poller

    rate = 15000  # msgs/s
    p = RateProducer(cluster, sched, topic, rate)

    try:
        # Wait until some messages have been successfully delivered
        # to partition 0.
        p.wait_partition_delivered(0, rate/2)

        # Get the leader for partition 0
        leader0 = p.partition_leaders()[0]
        delivcnt0 = p.partition_delivered_cnt(0)

        print("Partition 0 leader is {}, with "
              "{} messages delivered: stopping leader".format(
                  leader0, delivcnt0))

        # Stop the leader
        cluster.stop_broker(leader0)

        # Wait until some more messages are delivered
        # from the point the leader was brought down.
        p.wait_partition_delivered(0, delivcnt0 + (rate*2))

        newleader0 = p.partition_leaders()[0]
        delivcnt0 = p.partition_delivered_cnt(0)

        print("Partition 0 leader is now {}, with {} messages delivered: "
              "restarting previous leader {}".format(
                  newleader0, delivcnt0, leader0))

        # Then bring the broker back up
        cluster.start_broker(leader0)

        # And then take the new leader down
        cluster.stop_broker(newleader0)

        # Wait until some more messages are delivered
        # from the point the leader was brought up
        p.wait_partition_delivered(0, delivcnt0 + (rate*2))

        # Bring up the new leader again
        cluster.start_broker(newleader0)

        newleader0 = p.partition_leaders()[0]
        delivcnt0 = p.partition_delivered_cnt(0)
        print("Partition 0 leader is now {}, with {} "
              "messages delivered".format(newleader0, delivcnt0))

    except KeyboardInterrupt:
        print("Interrupted by user")

    p.stop()

    sched.shutdown()

    cluster.stop()


if __name__ == '__main__':
    test_broker_failover()
