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

import os
from json import JSONEncoder
from StringIO import StringIO

from . import exceptions
from ..parser.consumption import ConsumptionContext
from ..utils.console import puts
from ..utils.type import validate_value_type
from ..utils.collections import OrderedDict


class ModelJSONEncoder(JSONEncoder):
    def default(self, o):  # pylint: disable=method-hidden
        from .mixins import ModelMixin
        if isinstance(o, ModelMixin):
            if hasattr(o, 'value'):
                dict_to_return = o.to_dict(fields=('value',))
                return dict_to_return['value']
            else:
                return o.to_dict()
        else:
            return JSONEncoder.default(self, o)


class NodeTemplateContainerHolder(object):
    """
    Wrapper that allows using a :class:`aria.modeling.models.NodeTemplate` model directly as the
    ``container_holder`` input for :func:`aria.modeling.functions.evaluate`.
    """

    def __init__(self, node_template):
        self.container = node_template
        self.service = None

    @property
    def service_template(self):
        return self.container.service_template


def create_parameters(parameters, declared_parameters):
    """
    Validates, merges, and wraps parameter values according to those declared by a type.

    Exceptions will be raised for validation errors:

    * :class:`aria.modeling.exceptions.UndeclaredParametersException` if a key in ``parameters``
      does not exist in ``declared_parameters``
    * :class:`aria.modeling.exceptions.MissingRequiredParametersException` if a key in
      ``declared_parameters`` does not exist in ``parameters`` and also has no default value
    * :class:`aria.modeling.exceptions.ParametersOfWrongTypeException` if a value in ``parameters``
      does not match its type in ``declared_parameters``

    :param parameters: Provided parameter values
    :type parameters: {basestring, object}
    :param declared_parameters: Declared parameters
    :type declared_parameters: {basestring, :class:`aria.modeling.models.Parameter`}
    :return: The merged parameters
    :rtype: {basestring, :class:`aria.modeling.models.Parameter`}
    """

    merged_parameters = _merge_and_validate_parameters(parameters, declared_parameters)

    from . import models
    parameters_models = OrderedDict()
    for parameter_name, parameter_value in merged_parameters.iteritems():
        parameter = models.Parameter( # pylint: disable=unexpected-keyword-arg
            name=parameter_name,
            type_name=declared_parameters[parameter_name].type_name,
            description=declared_parameters[parameter_name].description,
            value=parameter_value)
        parameters_models[parameter.name] = parameter

    return parameters_models


def _merge_and_validate_parameters(parameters, declared_parameters):
    merged_parameters = OrderedDict(parameters)

    missing_parameters = []
    wrong_type_parameters = OrderedDict()
    for parameter_name, declared_parameter in declared_parameters.iteritems():
        if parameter_name not in parameters:
            if declared_parameter.value is not None:
                merged_parameters[parameter_name] = declared_parameter.value  # apply default value
            else:
                missing_parameters.append(parameter_name)
        else:
            # Validate parameter type
            try:
                validate_value_type(parameters[parameter_name], declared_parameter.type_name)
            except ValueError:
                wrong_type_parameters[parameter_name] = declared_parameter.type_name
            except RuntimeError:
                # TODO: This error shouldn't be raised (or caught), but right now we lack support
                # for custom data_types, which will raise this error. Skipping their validation.
                pass

    if missing_parameters:
        raise exceptions.MissingRequiredParametersException(
            'Required parameters {0} have not been specified; Expected parameters: {1}'
            .format(missing_parameters, declared_parameters.keys()))

    if wrong_type_parameters:
        error_message = StringIO()
        for param_name, param_type in wrong_type_parameters.iteritems():
            error_message.write('Parameter "{0}" must be of type {1}{2}'
                                .format(param_name, param_type, os.linesep))
        raise exceptions.ParametersOfWrongTypeException(error_message.getvalue())

    undeclared_parameters = [parameter_name for parameter_name in parameters.keys()
                             if parameter_name not in declared_parameters]
    if undeclared_parameters:
        raise exceptions.UndeclaredParametersException(
            'Undeclared parameters have been specified: {0}; Expected parameters: {1}'
            .format(undeclared_parameters, declared_parameters.keys()))

    return merged_parameters


def coerce_dict_values(the_dict, report_issues=False):
    if not the_dict:
        return
    coerce_list_values(the_dict.itervalues(), report_issues)


def coerce_list_values(the_list, report_issues=False):
    if not the_list:
        return
    for value in the_list:
        value.coerce_values(report_issues)


def validate_dict_values(the_dict):
    if not the_dict:
        return
    validate_list_values(the_dict.itervalues())


def validate_list_values(the_list):
    if not the_list:
        return
    for value in the_list:
        value.validate()


def instantiate_dict(container, the_dict, from_dict):
    if not from_dict:
        return
    for name, value in from_dict.iteritems():
        value = value.instantiate(container)
        if value is not None:
            the_dict[name] = value


def instantiate_list(container, the_list, from_list):
    if not from_list:
        return
    for value in from_list:
        value = value.instantiate(container)
        if value is not None:
            the_list.append(value)


def dump_list_values(the_list, name):
    if not the_list:
        return
    puts('%s:' % name)
    context = ConsumptionContext.get_thread_local()
    with context.style.indent:
        for value in the_list:
            value.dump()


def dump_dict_values(the_dict, name):
    if not the_dict:
        return
    dump_list_values(the_dict.itervalues(), name)


def dump_interfaces(interfaces, name='Interfaces'):
    if not interfaces:
        return
    puts('%s:' % name)
    context = ConsumptionContext.get_thread_local()
    with context.style.indent:
        for interface in interfaces.itervalues():
            interface.dump()


class classproperty(object):                                                                        # pylint: disable=invalid-name
    def __init__(self, f):
        self._func = f

    def __get__(self, instance, owner):
        return self._func(owner)
