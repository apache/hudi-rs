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

"""Check integration runner exit statuses without starting Docker."""

import os
import subprocess
import tempfile
import unittest
from pathlib import Path


class CiRunTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="hudi ci test ")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.log = self.root / "docker.log"
        self.script = Path(__file__).resolve().parents[1] / "ci_run.sh"
        docker = self.root / "docker"
        docker.write_text(
            """#!/bin/bash
if [[ "$1" == inspect ]]; then
    echo inspect >> "$DEMO_TEST_DOCKER_LOG"
    echo running
elif [[ "$1" == compose ]]; then
    echo "$2" >> "$DEMO_TEST_DOCKER_LOG"
    case "$2" in
        exec) exit "$DEMO_TEST_EXEC_STATUS" ;;
        down) exit "$DEMO_TEST_CLEANUP_STATUS" ;;
    esac
fi
exit 0
"""
        )
        docker.chmod(0o755)

    def run_runner(self, exec_status, cleanup_status=0, app="hudi-file-group-api/cpp"):
        self.log.unlink(missing_ok=True)
        env = dict(os.environ)
        env.update(
            PATH=str(self.root) + os.pathsep + env["PATH"],
            DEMO_TEST_DOCKER_LOG=str(self.log),
            DEMO_TEST_EXEC_STATUS=str(exec_status),
            DEMO_TEST_CLEANUP_STATUS=str(cleanup_status),
        )
        result = subprocess.run(
            ["bash", str(self.script), app],
            cwd=self.root,
            env=env,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            check=False,
            timeout=10,
        )
        self.assertEqual(self.log.read_text().splitlines(), ["up", "inspect", "exec", "down"])
        return result

    def test_success_returns_zero_and_cleans_up(self):
        result = self.run_runner(0)
        self.assertEqual(result.returncode, 0, result.stdout)

    def test_ctest_failure_is_not_hidden_by_successful_cleanup(self):
        result = self.run_runner(8)
        self.assertEqual(result.returncode, 8, result.stdout)

    def test_cleanup_failure_preserves_original_test_failure(self):
        result = self.run_runner(8, cleanup_status=9)
        self.assertEqual(result.returncode, 8, result.stdout)
        self.assertIn("Warning: Failed to tear down compose stack", result.stdout)

    def test_cleanup_failure_after_success_remains_a_warning(self):
        result = self.run_runner(0, cleanup_status=9)
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertIn("Warning: Failed to tear down compose stack", result.stdout)

    def test_other_demo_failures_are_preserved(self):
        for app in ("datafusion", "hudi-table-api/rust", "hudi-table-api/python"):
            with self.subTest(app=app):
                result = self.run_runner(37, app=app)
                self.assertEqual(result.returncode, 37, result.stdout)


if __name__ == "__main__":
    unittest.main()
