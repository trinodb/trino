/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.tests.product.launcher.cli;

import com.google.common.base.Splitter;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;

import static io.trino.tests.product.launcher.cli.Launcher.execute;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

@Test(singleThreaded = true)
public class TestInvocations
{
    @Test
    public void testEnvironmentList()
    {
        InvocationResult invocationResult = invokeLauncher("env", "list");

        assertThat(invocationResult.exitCode()).isEqualTo(0);
        assertThat(invocationResult.lines())
                .contains("Available environments: ")
                .contains("multinode")
                .contains("two-mixed-hives");
    }

    @Test
    public void testSuiteList()
    {
        InvocationResult invocationResult = invokeLauncher("suite", "list");

        assertThat(invocationResult.exitCode()).isEqualTo(0);
        assertThat(invocationResult.lines())
                .contains("Available suites: ")
                .contains("suite-1");
    }

    @Test
    public void testDescribeEnvironment()
            throws IOException
    {
        InvocationResult invocationResult = invokeLauncher(
                "env", "describe",
                "--server-package", Files.createTempFile("server", ".tar.gz").toString(),
                // This is known to work for both arm and x86
                "--environment", "multinode-postgresql");

        assertThat(invocationResult.exitCode()).isEqualTo(0);
        assertThat(invocationResult.lines())
                .contains("Environment 'multinode-postgresql' file mounts:");
    }

    @Test
    public void testDescribeSuite()
            throws IOException
    {
        InvocationResult invocationResult = invokeLauncher(
                "suite", "describe",
                "--server-package", Files.createTempFile("server", ".tar.gz").toString(),
                "--suite", "suite-1");

        assertThat(invocationResult.exitCode()).isEqualTo(0);
        assertThat(invocationResult.lines())
                .contains("Suite 'suite-1' with configuration 'config-default' consists of following test runs: ");
    }

    @Test
    public void testEnvUpDown()
            throws IOException
    {
        InvocationResult upResult = invokeLauncher(
                "env", "up",
                "--server-package", Files.createTempFile("server", ".tar.gz").toString(),
                "--without-trino",
                "--background",
                "--bind",
                "off",
                // This is known to work for both arm and x86
                "--environment", "multinode-postgresql");

        assertThat(upResult.exitCode()).isEqualTo(0);
        InvocationResult downResult = invokeLauncher("env", "down");
        assertThat(downResult.exitCode()).isEqualTo(0);
    }

    public static InvocationResult invokeLauncher(String... args)
    {
        Launcher launcher = new Launcher();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        int exitCode = execute(launcher, new Launcher.LauncherBundle(), out, args);
        return new InvocationResult(exitCode, Splitter.on("\n").splitToList(out.toString(UTF_8)));
    }

    @SuppressWarnings("UnusedVariable")
    private record InvocationResult(int exitCode, List<String> lines) {}
}
