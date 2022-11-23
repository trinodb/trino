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
import com.google.inject.Module;
import io.trino.tests.product.launcher.Extensions;
import io.trino.tests.product.launcher.LauncherModule;
import io.trino.tests.product.launcher.env.EnvironmentConfigFactory;
import io.trino.tests.product.launcher.env.EnvironmentFactory;
import io.trino.tests.product.launcher.env.EnvironmentModule;
import io.trino.tests.product.launcher.env.EnvironmentOptions;
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
import static picocli.CommandLine.Command;

@Command(
        name = "list",
        description = "List environments",
        usageHelpAutoWidth = true)
public final class EnvironmentList
        implements Callable<Integer>
{
    @Option(names = {"-h", "--help"}, usageHelp = true, description = "Show this help message and exit")
    public boolean usageHelpRequested;

    private final Module additionalEnvironments;

    public EnvironmentList(Extensions extensions)
    {
        this.additionalEnvironments = extensions.getAdditionalEnvironments();
    }

    @Override
    public Integer call()
    {
        return runCommand(
                ImmutableList.<Module>builder()
                        .add(new LauncherModule())
                        .add(new EnvironmentModule(EnvironmentOptions.empty(), additionalEnvironments))
                        .build(),
                EnvironmentList.Execution.class);
    }

    public static class Execution
            implements Callable<Integer>
    {
        private final PrintStream out;
        private final EnvironmentFactory factory;
        private final EnvironmentConfigFactory configFactory;

        @Inject
        public Execution(EnvironmentFactory factory, EnvironmentConfigFactory configFactory)
        {
            this.factory = requireNonNull(factory, "factory is null");
            this.configFactory = requireNonNull(configFactory, "configFactory is null");

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
            out.println("Available environments: ");
            this.factory.list().forEach(out::println);

            out.println("\nAvailable environment configs: ");
            this.configFactory.listConfigs().forEach(out::println);

            return ExitCode.OK;
        }
    }
}
