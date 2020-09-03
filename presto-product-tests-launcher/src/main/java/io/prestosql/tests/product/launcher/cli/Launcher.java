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
package io.prestosql.tests.product.launcher.cli;

import io.prestosql.tests.product.launcher.Extensions;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;
import picocli.CommandLine.IFactory;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;

import static com.google.common.base.MoreObjects.firstNonNull;
import static io.prestosql.tests.product.launcher.cli.Launcher.EnvironmentCommand;
import static io.prestosql.tests.product.launcher.cli.Launcher.SuiteCommand;
import static io.prestosql.tests.product.launcher.cli.Launcher.TestCommand;
import static io.prestosql.tests.product.launcher.cli.Launcher.VersionProvider;
import static java.util.Objects.requireNonNull;
import static picocli.CommandLine.IVersionProvider;
import static picocli.CommandLine.Spec;

@Command(
        name = "launcher",
        usageHelpAutoWidth = true,
        versionProvider = VersionProvider.class,
        subcommands = {
                HelpCommand.class,
                EnvironmentCommand.class,
                SuiteCommand.class,
                TestCommand.class,
        })
public class Launcher
{
    @Option(names = {"-h", "--help"}, usageHelp = true, description = "Show this help message and exit")
    public boolean usageHelpRequested;

    @Option(names = "--version", versionHelp = true, description = "Print version information and exit")
    public boolean versionInfoRequested;

    public static void main(String[] args)
    {
        Launcher launcher = new Launcher();
        IFactory factory = createFactory(launcher.getExtensions());
        CommandLine commandLine = new CommandLine(launcher, factory)
                .setUnmatchedOptionsArePositionalParams(true);
        System.exit(commandLine.execute(args));
    }

    private static IFactory createFactory(Extensions extensions)
    {
        requireNonNull(extensions, "extensions is null");
        return new IFactory()
        {
            @Override
            public <T> T create(Class<T> clazz)
                    throws Exception
            {
                try {
                    return clazz.getConstructor(Extensions.class).newInstance(extensions);
                }
                catch (NoSuchMethodException ignore) {
                    return CommandLine.defaultFactory().create(clazz);
                }
            }
        };
    }

    protected Extensions getExtensions()
    {
        return new Extensions(binder -> {});
    }

    @Command(
            name = "env",
            description = "Manage test environments",
            usageHelpAutoWidth = true,
            subcommands = {
                    HelpCommand.class,
                    EnvironmentUp.class,
                    EnvironmentDown.class,
                    EnvironmentList.class,
            })
    public static class EnvironmentCommand
    {
        @Option(names = {"-h", "--help"}, usageHelp = true, description = "Show this help message and exit")
        public boolean usageHelpRequested;
    }

    @Command(
            name = "suite",
            description = "Manage test suites",
            usageHelpAutoWidth = true,
            subcommands = {
                    HelpCommand.class,
                    SuiteRun.class,
                    SuiteList.class,
                    SuiteDescribe.class,
            })
    public static class SuiteCommand
    {
        @Option(names = {"-h", "--help"}, usageHelp = true, description = "Show this help message and exit")
        public boolean usageHelpRequested;
    }

    @Command(
            name = "test",
            description = "Run a Presto product test",
            usageHelpAutoWidth = true,
            subcommands = {
                    HelpCommand.class,
                    TestRun.class,
            })
    public static class TestCommand
    {
        @Option(names = {"-h", "--help"}, usageHelp = true, description = "Show this help message and exit")
        public boolean usageHelpRequested;
    }

    public static class VersionProvider
            implements IVersionProvider
    {
        @Spec
        public CommandSpec spec;

        @Override
        public String[] getVersion()
        {
            String version = getClass().getPackage().getImplementationVersion();
            return new String[] {spec.name() + " " + firstNonNull(version, "(version unknown)")};
        }
    }
}
