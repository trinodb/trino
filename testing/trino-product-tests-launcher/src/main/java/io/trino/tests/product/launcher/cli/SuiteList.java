package io.trino.tests.product.launcher.cli;

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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Module;
import io.trino.tests.product.launcher.Extensions;
import io.trino.tests.product.launcher.env.EnvironmentConfigFactory;
import io.trino.tests.product.launcher.env.EnvironmentModule;
import io.trino.tests.product.launcher.env.EnvironmentOptions;
import io.trino.tests.product.launcher.suite.SuiteFactory;
import io.trino.tests.product.launcher.suite.SuiteModule;
import picocli.CommandLine.Command;
import picocli.CommandLine.ExitCode;
import picocli.CommandLine.Option;

import java.io.OutputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.concurrent.Callable;

import static java.util.Objects.requireNonNull;

@Command(
        name = "list",
        description = "List tests suite",
        usageHelpAutoWidth = true)
public final class SuiteList
        extends LauncherCommand
{
    @Option(names = {"-h", "--help"}, usageHelp = true, description = "Show this help message and exit")
    public boolean usageHelpRequested;

    public SuiteList(OutputStream outputStream, Extensions extensions)
    {
        super(SuiteList.Execution.class, outputStream, extensions);
    }

    @Override
    List<Module> getCommandModules()
    {
        return ImmutableList.of(
                new EnvironmentModule(EnvironmentOptions.empty(), extensions.getAdditionalEnvironments()),
                new SuiteModule(extensions.getAdditionalSuites()));
    }

    public static class Execution
            implements Callable<Integer>
    {
        private final PrintStream printStream;
        private final EnvironmentConfigFactory configFactory;
        private final SuiteFactory suiteFactory;

        @Inject
        public Execution(PrintStream printStream, SuiteFactory suiteFactory, EnvironmentConfigFactory configFactory)
        {
            this.configFactory = requireNonNull(configFactory, "configFactory is null");
            this.suiteFactory = requireNonNull(suiteFactory, "suiteFactory is null");
            this.printStream = requireNonNull(printStream, "printStream is null");
        }

        @Override
        public Integer call()
        {
            printStream.println("Available suites: ");
            this.suiteFactory.listSuites().forEach(printStream::println);

            printStream.println("\nAvailable environment configs: ");
            this.configFactory.listConfigs().forEach(printStream::println);

            return ExitCode.OK;
        }
    }
}
