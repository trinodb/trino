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

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Module;
import io.trino.tests.product.launcher.Extensions;
import io.trino.tests.product.launcher.env.EnvironmentConfigFactory;
import io.trino.tests.product.launcher.env.EnvironmentFactory;
import io.trino.tests.product.launcher.env.EnvironmentModule;
import io.trino.tests.product.launcher.env.EnvironmentOptions;
import picocli.CommandLine.ExitCode;
import picocli.CommandLine.Option;

import java.io.OutputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.concurrent.Callable;

import static java.util.Objects.requireNonNull;
import static picocli.CommandLine.Command;

@Command(
        name = "list",
        description = "List environments",
        usageHelpAutoWidth = true)
public final class EnvironmentList
        extends LauncherCommand
{
    @Option(names = {"-h", "--help"}, usageHelp = true, description = "Show this help message and exit")
    public boolean usageHelpRequested;

    public EnvironmentList(OutputStream outputStream, Extensions extensions)
    {
        super(EnvironmentList.Execution.class, outputStream, extensions);
    }

    @Override
    List<Module> getCommandModules()
    {
        return ImmutableList.of(new EnvironmentModule(EnvironmentOptions.empty(), extensions.getAdditionalEnvironments()));
    }

    public static class Execution
            implements Callable<Integer>
    {
        private final PrintStream out;
        private final EnvironmentFactory factory;
        private final EnvironmentConfigFactory configFactory;

        @Inject
        public Execution(PrintStream out, EnvironmentFactory factory, EnvironmentConfigFactory configFactory)
        {
            this.factory = requireNonNull(factory, "factory is null");
            this.configFactory = requireNonNull(configFactory, "configFactory is null");
            this.out = requireNonNull(out, "out is null");
        }

        @Override
        public Integer call()
        {
            out.println("Available environments: ");
            this.factory.list().forEach(out::println);

            out.println("\nAvailable environment configs: ");
            this.configFactory.listConfigs().forEach(out::println);

            return ExitCode.OK;
        }
    }
}
