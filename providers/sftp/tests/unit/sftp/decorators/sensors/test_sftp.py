# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import annotations

from unittest.mock import patch

import pytest

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk import task
else:
    from airflow.decorators import task  # type: ignore[attr-defined,no-redef]
from airflow.utils import timezone

pytestmark = pytest.mark.db_test


DEFAULT_DATE = timezone.datetime(2021, 9, 9)


class TestSFTPDecoratorSensor:
    @patch("airflow.providers.sftp.sensors.sftp.SFTPHook")
    def test_decorator_with_file_path(self, sftp_hook_mock, dag_maker):
        sftp_hook_mock.return_value.get_mod_time.return_value = "19700101000000"
        file_path = "/path/to/file/2021-09-09.txt"
        decorated_func_return = "decorated_func_returns"
        expected_xcom_return = {"files_found": [file_path], "decorator_return_value": decorated_func_return}

        @task.sftp_sensor(path=file_path)
        def f():
            return decorated_func_return

        with dag_maker():
            ret = f()

        assert ret.operator.execute({}) == expected_xcom_return

    @patch("airflow.providers.sftp.sensors.sftp.SFTPHook")
    def test_decorator_with_file_path_with_args(self, sftp_hook_mock, dag_maker):
        sftp_hook_mock.return_value.get_mod_time.return_value = "19700101000000"
        file_path = "/path/to/file/1970-01-01.txt"
        op_args = ("op_args_1",)
        op_kwargs = {"key": "value"}
        decorated_func_return = {"args": op_args, "kwargs": {**op_kwargs, "files_found": [file_path]}}
        expected_xcom_return = {"files_found": [file_path], "decorator_return_value": decorated_func_return}

        @task.sftp_sensor(path=file_path)
        def f(*args, **kwargs):
            return {"args": args, "kwargs": kwargs}

        with dag_maker():
            ret = f(*op_args, **op_kwargs)

        assert ret.operator.execute({}) == expected_xcom_return

    @patch("airflow.providers.sftp.sensors.sftp.SFTPHook")
    def test_decorator_with_file_pattern(self, sftp_hook_mock, dag_maker):
        sftp_hook_mock.return_value.get_mod_time.return_value = "19700101000000"
        file_path_list = ["/path/to/file/text_file.txt", "/path/to/file/another_text_file.txt"]
        sftp_hook_mock.return_value.get_files_by_pattern.return_value = [
            "text_file.txt",
            "another_text_file.txt",
        ]
        decorated_func_return = "decorated_func_returns"
        expected_xcom_return = {
            "files_found": file_path_list,
            "decorator_return_value": decorated_func_return,
        }

        @task.sftp_sensor(path="/path/to/file/", file_pattern=".txt")
        def f():
            return decorated_func_return

        with dag_maker():
            ret = f()

        assert ret.operator.execute({}) == expected_xcom_return

    @patch("airflow.providers.sftp.sensors.sftp.SFTPHook")
    def test_decorator_with_file_pattern_with_args(self, sftp_hook_mock, dag_maker):
        sftp_hook_mock.return_value.get_mod_time.return_value = "19700101000000"
        file_path_list = ["/path/to/file/text_file.txt", "/path/to/file/another_text_file.txt"]
        op_args = ("op_args_1",)
        op_kwargs = {"key": "value"}
        sftp_hook_mock.return_value.get_files_by_pattern.return_value = [
            "text_file.txt",
            "another_text_file.txt",
        ]
        decorated_func_return = {"args": op_args, "kwargs": {**op_kwargs, "files_found": file_path_list}}
        expected_xcom_return = {
            "files_found": file_path_list,
            "decorator_return_value": decorated_func_return,
        }

        @task.sftp_sensor(path="/path/to/file/", file_pattern=".txt")
        def f(*args, **kwargs):
            return {"args": args, "kwargs": kwargs}

        with dag_maker():
            ret = f(*op_args, **op_kwargs)

        assert ret.operator.execute({}) == expected_xcom_return
