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

import io.airlift.units.Duration;
import io.trino.tests.product.launcher.Extensions;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;
import picocli.CommandLine.IFactory;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;

import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ListResourceBundle;
import java.util.ResourceBundle;

import static com.google.inject.util.Modules.EMPTY_MODULE;
import static io.trino.testing.TestingProperties.getProjectVersion;
import static io.trino.tests.product.launcher.cli.Launcher.EnvironmentCommand;
import static io.trino.tests.product.launcher.cli.Launcher.SuiteCommand;
import static io.trino.tests.product.launcher.cli.Launcher.TestCommand;
import static io.trino.tests.product.launcher.cli.Launcher.VersionProvider;
import static java.lang.String.format;
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
        // write directly to System.out, bypassing logging & io.airlift.log.Logging#rewireStdStreams
        System.exit(execute(launcher, new LauncherBundle(), new FileOutputStream(FileDescriptor.out), args));
    }

    public static int execute(Launcher launcher, ResourceBundle bundle, OutputStream outputStream, String[] args)
    {
        IFactory factory = createFactory(outputStream, launcher.getExtensions());
        return new CommandLine(launcher, factory)
                .setCaseInsensitiveEnumValuesAllowed(true)
                .registerConverter(Duration.class, Duration::valueOf)
                .registerConverter(Path.class, Paths::get)
                .setResourceBundle(bundle)
                .execute(args);
    }

    private static IFactory createFactory(OutputStream outputStream, Extensions extensions)
    {
        requireNonNull(extensions, "extensions is null");
        return new IFactory()
        {
            @Override
            public <T> T create(Class<T> clazz)
                    throws Exception
            {
                try {
                    return clazz.getConstructor(OutputStream.class, Extensions.class).newInstance(outputStream, extensions);
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
            return new String[] {spec.name() + " " + getProjectVersion()};
        }
    }

    static class LauncherBundle
            extends ListResourceBundle
    {
        @Override
        protected Object[][] getContents()
        {
            try {
                Path jdkDistribution = findJdkDistribution();
                return new Object[][] {
                        {"project.version", getProjectVersion()},
                        {"product-tests.module", "testing/trino-product-tests"},
                        {"product-tests.name", "trino-product-tests"},
                        {"server.package", "core/trino-server/target/trino-server-" + getProjectVersion() + ".tar.gz"},
                        {"launcher.bin", "testing/trino-product-tests-launcher/bin/run-launcher"},
                        {"cli.bin", format("client/trino-cli/target/trino-cli-%s-executable.jar", getProjectVersion())},
                        {"jdk.current", Files.readString(jdkDistribution).trim()},
                        {"jdk.distributions", jdkDistribution.getParent().toAbsolutePath().toString()}
                };
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        protected Path findJdkDistribution()
        {
            String searchFor = "core/jdk/current";
            Path currentWorkingDirectory = Paths.get("").toAbsolutePath();
            Path current = currentWorkingDirectory; // current working directory

            while (current != null) {
                if (Files.exists(current.resolve(searchFor))) {
                    return current.resolve(searchFor);
                }

                current = current.getParent();
            }

            throw new RuntimeException("Could not find %s in the directory %s and its' parents".formatted(searchFor, currentWorkingDirectory));
        }
    }
}
