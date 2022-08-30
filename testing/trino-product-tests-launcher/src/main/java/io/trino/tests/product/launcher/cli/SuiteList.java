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
import com.google.inject.Module;
import io.trino.tests.product.launcher.Extensions;
import io.trino.tests.product.launcher.LauncherModule;
import io.trino.tests.product.launcher.env.EnvironmentConfigFactory;
import io.trino.tests.product.launcher.env.EnvironmentModule;
import io.trino.tests.product.launcher.env.EnvironmentOptions;
import io.trino.tests.product.launcher.suite.SuiteFactory;
import io.trino.tests.product.launcher.suite.SuiteModule;
import picocli.CommandLine.Command;
import picocli.CommandLine.ExitCode;
import picocli.CommandLine.Option;

import javax.inject.Inject;

import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.concurrent.Callable;

import static io.trino.tests.product.launcher.cli.Commands.runCommand;
import static java.util.Objects.requireNonNull;

@Command(
        name = "list",
        description = "List tests suite",
        usageHelpAutoWidth = true)
public final class SuiteList
        implements Callable<Integer>
{
    private final Module additionalEnvironments;
    private final Module additionalSuites;

    @Option(names = {"-h", "--help"}, usageHelp = true, description = "Show this help message and exit")
    public boolean usageHelpRequested;

    public SuiteList(Extensions extensions)
    {
        this.additionalEnvironments = extensions.getAdditionalEnvironments();
        this.additionalSuites = extensions.getAdditionalSuites();
    }

    @Override
    public Integer call()
    {
        return runCommand(
                ImmutableList.<Module>builder()
                        .add(new LauncherModule())
                        .add(new EnvironmentModule(EnvironmentOptions.empty(), additionalEnvironments))
                        .add(new SuiteModule(additionalSuites))
                        .build(),
                SuiteList.Execution.class);
    }

    public static class Execution
            implements Callable<Integer>
    {
        private final PrintStream out;
        private final EnvironmentConfigFactory configFactory;
        private final SuiteFactory suiteFactory;

        @Inject
        public Execution(SuiteFactory suiteFactory, EnvironmentConfigFactory configFactory)
        {
            this.configFactory = requireNonNull(configFactory, "configFactory is null");
            this.suiteFactory = requireNonNull(suiteFactory, "suiteFactory is null");

            try {
                this.out = new PrintStream(new FileOutputStream(FileDescriptor.out), true, Charset.defaultCharset().name());
            }
            catch (UnsupportedEncodingException e) {
                throw new IllegalStateException("Could not create print stream", e);
            }
        }

        @Override
        public Integer call()
        {
            out.println("Available suites: ");
            this.suiteFactory.listSuites().forEach(out::println);

            out.println("\nAvailable environment configs: ");
            this.configFactory.listConfigs().forEach(out::println);

            return ExitCode.OK;
        }
    }
}
