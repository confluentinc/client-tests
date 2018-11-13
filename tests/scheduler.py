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

import logging
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor


loghandler = logging.StreamHandler()
loghandler.setFormatter(logging.Formatter(
    '%(asctime)-15s %(levelname)-8s %(message)s'))

schedlogger = logging.getLogger('apscheduler.executors.default')
schedlogger.setLevel(logging.ERROR)
schedlogger.addHandler(loghandler)
schedlogger2 = logging.getLogger('apscheduler.scheduler')
schedlogger2.setLevel(logging.ERROR)
schedlogger2.addHandler(loghandler)


class ThreadScheduler(BackgroundScheduler):
    def __init__(self, num_workers):
        super(ThreadScheduler, self).__init__(
            executors={
                'default': ThreadPoolExecutor(num_workers)
            })

        self.start()
