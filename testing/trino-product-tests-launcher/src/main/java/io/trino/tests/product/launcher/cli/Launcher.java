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

import com.google.common.io.Resources;
import io.airlift.units.Duration;
import io.trino.tests.product.launcher.Extensions;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;
import picocli.CommandLine.IFactory;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ListResourceBundle;

import static com.google.common.base.Preconditions.checkState;
import static com.google.inject.util.Modules.EMPTY_MODULE;
import static io.trino.tests.product.launcher.cli.Launcher.EnvironmentCommand;
import static io.trino.tests.product.launcher.cli.Launcher.SuiteCommand;
import static io.trino.tests.product.launcher.cli.Launcher.TestCommand;
import static io.trino.tests.product.launcher.cli.Launcher.VersionProvider;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
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
        run(launcher, args);
    }

    public static void run(Launcher launcher, String[] args)
    {
        IFactory factory = createFactory(launcher.getExtensions());
        System.exit(new CommandLine(launcher, factory)
                .setCaseInsensitiveEnumValuesAllowed(true)
                .registerConverter(Duration.class, Duration::valueOf)
                .registerConverter(Path.class, Paths::get)
                .setResourceBundle(new LauncherBundle()).execute(args));
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
        return new Extensions(EMPTY_MODULE);
    }

    @Command(
            name = "env",
            description = "Manage test environments",
            usageHelpAutoWidth = true,
            subcommands = {
                    HelpCommand.class,
                    EnvironmentUp.class,
                    EnvironmentDescribe.class,
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
            description = "Run a Trino product test",
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
            return new String[] {spec.name() + " " + readProjectVersion()};
        }
    }

    private static class LauncherBundle
            extends ListResourceBundle
    {
        @Override
        protected Object[][] getContents()
        {
            String projectVersion = readProjectVersion();

            return new Object[][] {
                    {"project.version", readProjectVersion()},
                    {"product-tests.module", "testing/trino-product-tests"},
                    {"product-tests.name", "trino-product-tests"},
                    {"server.module", "core/trino-server"},
                    {"server.name", "trino-server"},
                    {"launcher.bin", "testing/trino-product-tests-launcher/bin/run-launcher"},
                    {"cli.bin", format("client/trino-cli/target/trino-cli-%s-executable.jar", projectVersion)}
            };
        }
    }

    private static String readProjectVersion()
    {
        try {
            String version = Resources.toString(Resources.getResource("presto-product-tests-launcher-version.txt"), UTF_8).trim();
            checkState(!version.isEmpty(), "version is empty");
            return version;
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
