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
A common context for both workflow and operation
"""
import logging
from contextlib import contextmanager
from functools import partial

import jinja2

from aria import (
    logger as aria_logger,
    modeling
)
from aria.storage import exceptions
from aria.modeling import models

from ...utils.uuid import generate_uuid


class BaseContext(object):
    """
    Base context object for workflow and operation
    """

    class PrefixedLogger(object):
        def __init__(self, base_logger, task_id=None):
            self._logger = base_logger
            self._task_id = task_id

        def __getattr__(self, attribute):
            if attribute.upper() in logging._levelNames:
                return partial(self._logger_with_task_id, _level=attribute)
            else:
                return getattr(self._logger, attribute)

        def _logger_with_task_id(self, *args, **kwargs):
            level = kwargs.pop('_level')
            kwargs.setdefault('extra', {})['task_id'] = self._task_id
            return getattr(self._logger, level)(*args, **kwargs)

    def __init__(self,
                 name,
                 service_id,
                 model_storage,
                 resource_storage,
                 execution_id,
                 workdir=None,
                 **kwargs):
        super(BaseContext, self).__init__(**kwargs)
        self._name = name
        self._id = generate_uuid(variant='uuid')
        self._model = model_storage
        self._resource = resource_storage
        self._service_id = service_id
        self._workdir = workdir
        self._execution_id = execution_id
        self.logger = None

    def _register_logger(self, level=None, task_id=None):
        self.logger = self.PrefixedLogger(
            logging.getLogger(aria_logger.TASK_LOGGER_NAME), task_id=task_id)
        self.logger.setLevel(level or logging.DEBUG)
        if not self.logger.handlers:
            self.logger.addHandler(self._get_sqla_handler())

    def _get_sqla_handler(self):
        return aria_logger.create_sqla_log_handler(model=self._model,
                                                   log_cls=modeling.models.Log,
                                                   execution_id=self._execution_id)

    def __repr__(self):
        return (
            '{name}(name={self.name}, '
            'deployment_id={self._service_id}, '
            .format(name=self.__class__.__name__, self=self))

    @contextmanager
    def logging_handlers(self, handlers=None):
        handlers = handlers or []
        try:
            for handler in handlers:
                self.logger.addHandler(handler)
            yield self.logger
        finally:
            for handler in handlers:
                self.logger.removeHandler(handler)

    @property
    def model(self):
        """
        Access to the model storage
        :return:
        """
        return self._model

    @property
    def resource(self):
        """
        Access to the resource storage
        :return:
        """
        return self._resource

    @property
    def service_template(self):
        """
        The blueprint model
        """
        return self.service.service_template

    @property
    def service(self):
        """
        The deployment model
        """
        return self.model.service.get(self._service_id)

    @property
    def name(self):
        """
        The operation name
        :return:
        """
        return self._name

    @property
    def id(self):
        """
        The operation id
        :return:
        """
        return self._id

    def download_resource(self, destination, path=None):
        """
        Download a blueprint resource from the resource storage
        """
        try:
            self.resource.service.download(entry_id=str(self.service.id),
                                           destination=destination,
                                           path=path)
        except exceptions.StorageError:
            self.resource.service_template.download(entry_id=str(self.service_template.id),
                                                    destination=destination,
                                                    path=path)

    def download_resource_and_render(self, destination, path=None, variables=None):
        """
        Download a blueprint resource from the resource storage render its content as a jinja
        template using the provided variables. ctx is available to the template without providing it
        explicitly.
        """
        resource_content = self.get_resource(path=path)
        resource_content = self._render_resource(resource_content=resource_content,
                                                 variables=variables)
        with open(destination, 'wb') as f:
            f.write(resource_content)

    def get_resource(self, path=None):
        """
        Read a deployment resource as string from the resource storage
        """
        try:
            return self.resource.service.read(entry_id=str(self.service.id), path=path)
        except exceptions.StorageError:
            return self.resource.service_template.read(entry_id=str(self.service_template.id),
                                                       path=path)

    def get_resource_and_render(self, path=None, variables=None):
        """
        Read a deployment resource as string from the resource storage and render it as a jinja
        template using the provided variables. ctx is available to the template without providing it
        explicitly.
        """
        resource_content = self.get_resource(path=path)
        return self._render_resource(resource_content=resource_content, variables=variables)

    def _render_resource(self, resource_content, variables):
        variables = variables or {}
        variables.setdefault('ctx', self)
        resource_template = jinja2.Template(resource_content)
        return resource_template.render(variables)


class _InstrumentedCollection(object):

    def __init__(self,
                 model,
                 parent,
                 field_name,
                 seq=None,
                 is_top_level=True,
                 **kwargs):
        self._model = model
        self._parent = parent
        self._field_name = field_name
        self._is_top_level = is_top_level
        self._load(seq, **kwargs)

    @property
    def _raw(self):
        raise NotImplementedError

    def _load(self, seq, **kwargs):
        """
        Instantiates the object from existing seq.

        :param seq: the original sequence to load from
        :return:
        """
        raise NotImplementedError

    def _set(self, key, value):
        """
        set the changes for the current object (not in the db)

        :param key:
        :param value:
        :return:
        """
        raise NotImplementedError

    def _del(self, collection, key):
        raise NotImplementedError

    def _instrument(self, key, value):
        """
        Instruments any collection to track changes (and ease of access)
        :param key:
        :param value:
        :return:
        """
        if isinstance(value, _InstrumentedCollection):
            return value
        elif isinstance(value, dict):
            instrumentation_cls = _InstrumentedDict
        elif isinstance(value, list):
            instrumentation_cls = _InstrumentedList
        else:
            return value

        return instrumentation_cls(self._model, self, key, value, False)

    @staticmethod
    def _raw_value(value):
        """
        Get the raw value.
        :param value:
        :return:
        """
        if isinstance(value, models.Parameter):
            return value.value
        return value

    @staticmethod
    def _encapsulate_value(key, value):
        """
        Create a new item cls if needed.
        :param key:
        :param value:
        :return:
        """
        if isinstance(value, models.Parameter):
            return value
        # If it is not wrapped
        return models.Parameter.wrap(key, value)

    def __setitem__(self, key, value):
        """
        Update the values in both the local and the db locations.
        :param key:
        :param value:
        :return:
        """
        self._set(key, value)
        if self._is_top_level:
            # We are at the top level
            field = getattr(self._parent, self._field_name)
            mapi = getattr(self._model, models.Parameter.__modelname__)
            value = self._set_field(field,
                                    key,
                                    value if key in field else self._encapsulate_value(key, value))
            mapi.update(value)
        else:
            # We are not at the top level
            self._set_field(self._parent, self._field_name, self)

    def _set_field(self, collection, key, value):
        """
        enables updating the current change in the ancestors
        :param collection: the collection to change
        :param key: the key for the specific field
        :param value: the new value
        :return:
        """
        if isinstance(value, _InstrumentedCollection):
            value = value._raw
        if key in collection and isinstance(collection[key], models.Parameter):
            if isinstance(collection[key], _InstrumentedCollection):
                self._del(collection, key)
            collection[key].value = value
        else:
            collection[key] = value
        return collection[key]

    def __deepcopy__(self, *args, **kwargs):
        return self._raw


class _InstrumentedDict(_InstrumentedCollection, dict):

    def _load(self, dict_=None, **kwargs):
        dict.__init__(
            self,
            tuple((key, self._raw_value(value)) for key, value in (dict_ or {}).items()),
            **kwargs)

    def update(self, dict_=None, **kwargs):
        dict_ = dict_ or {}
        for key, value in dict_.items():
            self[key] = value
        for key, value in kwargs.items():
            self[key] = value

    def __getitem__(self, key):
        return self._instrument(key, dict.__getitem__(self, key))

    def _set(self, key, value):
        dict.__setitem__(self, key, self._raw_value(value))

    @property
    def _raw(self):
        return dict(self)

    def _del(self, collection, key):
        del collection[key]


class _InstrumentedList(_InstrumentedCollection, list):

    def _load(self, list_=None, **kwargs):
        list.__init__(self, list(item for item in list_ or []))

    def append(self, value):
        self.insert(len(self), value)

    def insert(self, index, value):
        list.insert(self, index, self._raw_value(value))
        if self._is_top_level:
            field = getattr(self._parent, self._field_name)
            field.insert(index, self._encapsulate_value(index, value))
        else:
            self._parent[self._field_name] = self

    def __getitem__(self, key):
        return self._instrument(key, list.__getitem__(self, key))

    def _set(self, key, value):
        list.__setitem__(self, key, value)

    def _del(self, collection, key):
        del collection[key]

    @property
    def _raw(self):
        return list(self)


class InstrumentCollection(object):

    def __init__(self, field_name):
        super(InstrumentCollection, self).__init__()
        self._field_name = field_name
        self._actor = None

    @property
    def actor(self):
        return self._actor

    def __getattr__(self, item):
        return getattr(self._actor, item)

    def __call__(self, func, *args, **kwargs):
        def _wrapper(func_self, *args, **kwargs):
            self._actor = func(func_self, *args, **kwargs)
            field = getattr(self._actor, self._field_name)

            # Preserve the original value. e.g. original attributes would be located under
            # _attributes
            setattr(self, '_{0}'.format(self._field_name), field)

            # set instrumented value
            setattr(self, self._field_name, _InstrumentedDict(func_self.model,
                                                              self._actor,
                                                              self._field_name,
                                                              field))
            return self
        return _wrapper
