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

import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;

import static io.trino.tests.product.launcher.cli.Launcher.execute;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

@Test(singleThreaded = true)
public class TestInvocations
{
    @Test
    public void testEnvironmentList()
    {
        InvocationResult invocationResult = invokeLauncher("env", "list");

        assertThat(invocationResult.exitCode()).isEqualTo(0);
        assertThat(invocationResult.output())
                .contains("Available environments:")
                .contains("multinode");
    }

    @Test
    public void testSuiteList()
    {
        InvocationResult invocationResult = invokeLauncher("suite", "list");

        assertThat(invocationResult.exitCode()).isEqualTo(0);
        assertThat(invocationResult.output())
                .contains("Available suites:")
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
        assertThat(invocationResult.output())
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
        assertThat(invocationResult.output())
                .contains("Suite 'suite-1' with configuration 'config-default' consists of following test runs");
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

        try (CaptureOutput ignored = new CaptureOutput(out)) {
            int exitCode = execute(launcher, new Launcher.LauncherBundle(), args);
            return new InvocationResult(exitCode, out.toString(UTF_8));
        }
    }

    @SuppressWarnings("UnusedVariable")
    private record InvocationResult(int exitCode, String output) {}

    private static class CaptureOutput
            implements AutoCloseable
    {
        private final PrintStream originalOut;
        private final PrintStream originalErr;

        public CaptureOutput(ByteArrayOutputStream out)
        {
            PrintStream stream = new PrintStream(requireNonNull(out, "out is null"));
            this.originalOut = System.out;
            this.originalErr = System.err;
            System.setOut(stream);
            System.setErr(stream);
        }

        @Override
        public void close()
        {
            System.setOut(originalOut);
            System.setErr(originalErr);
        }
    }
}
