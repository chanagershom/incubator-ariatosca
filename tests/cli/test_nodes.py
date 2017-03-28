import pytest
from mock import ANY

from aria.cli.env import _Environment
from tests.cli.base_test import TestCliBase, mock_storage  # pylint: disable=unused-import


class TestNodesShow(TestCliBase):

    def test_no_attributes(self, monkeypatch, mock_storage):

        monkeypatch.setattr(_Environment, 'model_storage', mock_storage)
        self.invoke('nodes show 1')
        assert 'Showing node 1' in self.logger_output_string
        assert 'Node:' in self.logger_output_string
        assert 'Node attributes:' in self.logger_output_string
        assert 'No attributes' in self.logger_output_string
        assert 'attribute1' not in self.logger_output_string
        assert 'value1' not in self.logger_output_string

    def test_one_attribute(self, monkeypatch, mock_storage):

        monkeypatch.setattr(_Environment, 'model_storage', mock_storage)
        self.invoke('nodes show 2')
        assert 'Showing node 2' in self.logger_output_string
        assert 'Node:' in self.logger_output_string
        assert 'Node attributes:' in self.logger_output_string
        assert 'No attributes' not in self.logger_output_string
        assert 'attribute1' in self.logger_output_string and 'value1' in self.logger_output_string


class TestNodesList(TestCliBase):

    @pytest.mark.parametrize('sort_by, order, sort_by_in_output, order_in_output', [
        ('', '', 'service_name', 'asc'),
        ('', ' --descending', 'service_name', 'desc'),
        (' --sort-by name', '', 'name', 'asc'),
        (' --sort-by name', ' --descending', 'name', 'desc')
    ])
    def test_list_specified_service(self, monkeypatch, mock_storage, sort_by, order,
                                    sort_by_in_output, order_in_output):

        monkeypatch.setattr(_Environment, 'model_storage', mock_storage)
        self.invoke('nodes list -s test_s{sort_by}{order}'.format(sort_by=sort_by,
                                                                  order=order))
        assert 'Listing nodes for service test_s...' in self.logger_output_string
        assert 'Listing all nodes...' not in self.logger_output_string

        nodes_list = mock_storage.node.list
        nodes_list.assert_called_once_with(sort={sort_by_in_output: order_in_output},
                                           filters={'service': ANY})
        assert 'Nodes:' in self.logger_output_string
        assert 'test_s' in self.logger_output_string
        assert 'test_n' in self.logger_output_string

    @pytest.mark.parametrize('sort_by, order, sort_by_in_output, order_in_output', [
        ('', '', 'service_name', 'asc'),
        ('', ' --descending', 'service_name', 'desc'),
        (' --sort-by name', '', 'name', 'asc'),
        (' --sort-by name', ' --descending', 'name', 'desc')
    ])
    def test_list_no_specified_service(self, monkeypatch, mock_storage, sort_by, order,
                                       sort_by_in_output, order_in_output):

        monkeypatch.setattr(_Environment, 'model_storage', mock_storage)
        self.invoke('nodes list{sort_by}{order}'.format(sort_by=sort_by,
                                                        order=order))
        assert 'Listing nodes for service test_s...' not in self.logger_output_string
        assert 'Listing all nodes...' in self.logger_output_string

        nodes_list = mock_storage.node.list
        nodes_list.assert_called_once_with(sort={sort_by_in_output: order_in_output},
                                           filters={})
        assert 'Nodes:' in self.logger_output_string
        assert 'test_s' in self.logger_output_string
        assert 'test_n' in self.logger_output_string
