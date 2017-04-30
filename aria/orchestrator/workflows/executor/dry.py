# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Dry executor
"""

from aria.orchestrator import events
from .base import BaseExecutor


# TODO: the name of this module should definitely change

class MarkerExecutor(BaseExecutor):
    def execute(self, task):
        task.start()
        task.end()


class StubExecutor(BaseExecutor):
    def execute(self, task):
        events.start_task_signal.send(task)
        events.on_success_task_signal.send(task)


class DryExecutor(BaseExecutor):
    """
    Executor which dry runs tasks - prints task information without causing any side effects
    """

    def execute(self, task):
        events.start_task_signal.send(task, skip_logging=True)

        if hasattr(task.actor, 'source_node'):
            name = '{source_node.name}->{target_node.name}'.format(
                source_node=task.actor.source_node, target_node=task.actor.target_node)
        else:
            name = task.actor.name

        task.context.logger.info(
            '<dry> {name} {task.interface_name}.{task.operation_name} started...'
            .format(name=name, task=task))

        task.context.logger.info(
            '<dry> {name} {task.interface_name}.{task.operation_name} successful'
            .format(name=name, task=task))

        events.on_success_task_signal.send(task, skip_logging=True)
