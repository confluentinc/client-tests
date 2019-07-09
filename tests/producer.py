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

import time
import logging
from collections import defaultdict
import confluent_kafka

from scheduler import loghandler


class RateProducer(object):
    """ Kafka Idempotent Producer producing messages at a specified rate """
    def __init__(self, cluster, sched, topic, rate):
        super(RateProducer, self).__init__()

        conf = cluster.client_config()
        conf.update({
            'enable.idempotence': True,
            'linger.ms': 50,
            # 'debug': 'eos,msg,protocol,broker,topic'
        })

        self.run = True
        self.p = confluent_kafka.Producer(conf)

        self.topic = topic
        self.cluster = cluster
        self.msgid = 0
        self.cnt = {'dr_err': 0, 'dr_ok': 0, 'err': 0}
        self.partcnt = defaultdict(dict)

        self.logger = logging.getLogger('RateProducer')
        self.logger.setLevel(logging.DEBUG)
        self.logger.addHandler(loghandler)

        self.sched = sched
        self.jobs = []
        self.jobs.append(sched.add_job(self.send_msg, 'interval',
                                       seconds=1.0/rate))
        self.jobs.append(sched.add_job(self.poll))

    def send_msg(self):
        val = "msg {} ".format(self.msgid+1) + ("pad" * 10)
        try:
            self.p.produce(topic=self.topic, value=val, on_delivery=self.dr_cb)
            self.msgid += 1
            # self.logger.debug("Produced {}".format(val))
        except Exception as e:
            self.logger.error("produce() #{} failed: {}".format(self.msgid, e))
            self.stop()

    def dr_cb(self, err, msg):
        if msg.partition() not in self.partcnt:
            self.partcnt[msg.partition()] = {'dr_err': 0, 'dr_ok': 0, 'err': 0}
        if err is not None:
            self.cnt['dr_err'] += 1
            self.partcnt[msg.partition()]['dr_err'] += 1
            self.logger.warning("Delivery of {} failed: {}".format(
                msg.value(), err))
        else:
            # self.logger.debug("Delivered {}".format(msg.value()))
            self.cnt['dr_ok'] += 1
            self.partcnt[msg.partition()]['dr_ok'] += 1

    def error_cb(self, err):
        if err.fatal():
            self.logger.fatal("Fatal error: {}".format(err))
            self.stop()
        else:
            self.logger.error("Error: {}".format(err))
        self.cnt['err'] += 1

    def poll(self):
        self.logger.info("Poller")
        while self.run:
            self.p.poll(0.5)

    def stop(self):
        self.run = False
        for j in self.jobs:
            try:
                self.sched.remove_job(j.id)
            except Exception as e:
                self.logger.warning("Ignoring remove_job exception: {}: "
                                    "for job {}".format(e, j))
                pass
        self.logger.info("Stopping producer and calling flush with {} "
                         "messages queued".format(len(self.p)))
        self.p.flush(30)
        if self.msgid != self.cnt['dr_ok']:
            self.logger.error("Error: {} messages produced, {} "
                              "delivered successfully, {} failed delivery, "
                              "{} not yet finished delivery".format(
                                  self.msgid, self.cnt['dr_ok'],
                                  self.cnt['dr_err'],
                                  self.msgid - self.cnt['dr_ok'] -
                                  self.cnt['dr_err']))
        else:
            self.logger.info("Success: {} messages produced, {} "
                             "delivered successfully, {} failed delivery, "
                             "{} not yet finished delivery".format(
                                 self.msgid, self.cnt['dr_ok'],
                                 self.cnt['dr_err'],
                                 self.msgid - self.cnt['dr_ok'] -
                                 self.cnt['dr_err']))

    def delivered_cnt(self):
        """ Returns the total number of successfully delivered messages """
        return self.cnt['dr_ok']

    def partition_delivered_cnt(self, partition):
        """ Same as delivered_cnt but for a single partition """
        if partition not in self.partcnt:
            return 0
        return self.partcnt[partition].get('dr_ok', 0)

    def wait_delivered(self, cnt):
        """ Wait until the total number of successfully delivered messages
        reaches the provided threshold. """
        while self.delivered_cnt() < cnt:
            time.sleep(0.3)

    def wait_partition_delivered(self, partition, cnt):
        """ Same as wait_delivered but for a single partition """
        self.logger.info("Waiting for a total of {} messages delivered "
                         "on partition {}".format(cnt, partition))
        while self.partition_delivered_cnt(partition) < cnt:
            time.sleep(0.3)

    def partition_leaders(self):
        """ Returns the partition leaders for topic's partitions as
            a key=partition, value=leader dict """
        cm = self.p.list_topics(self.topic)
        if self.topic not in cm.topics:
            raise LookupError("Topic {} not found in list_topics result: {}".
                              format(self.topic, cm))
        return {x: y.leader for x, y in
                cm.topics[self.topic].partitions.iteritems()}


def simple_produce_messages(cluster, topic, count, partition=None,
                            value_format="msg-{}"):
    """ Produce count messages to topic """
    conf = cluster.client_config()
    conf.update({'linger.ms': 5})

    p = confluent_kafka.Producer(conf)
    for n in range(0, count):
        while True:
            try:
                p.produce(topic=topic, partition=partition,
                          value=value_format.format(n))
                break
            except BufferError:
                p.poll(1)
                continue
    p.flush(30)

    print("Produced {} messages to {} [{}]".format(count, topic, partition))

