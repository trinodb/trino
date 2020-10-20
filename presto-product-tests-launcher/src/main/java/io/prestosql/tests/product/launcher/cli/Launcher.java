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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.io.PatternFilenameFilter;
import com.google.common.io.Resources;
import com.google.inject.Module;
import io.prestosql.tests.product.launcher.Extensions;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.HelpCommand;
import picocli.CommandLine.IFactory;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ListResourceBundle;
import java.util.ServiceLoader;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.configuration.ConfigurationAwareModule.combine;
import static io.prestosql.tests.product.launcher.cli.Launcher.EnvironmentCommand;
import static io.prestosql.tests.product.launcher.cli.Launcher.SuiteCommand;
import static io.prestosql.tests.product.launcher.cli.Launcher.TestCommand;
import static io.prestosql.tests.product.launcher.cli.Launcher.VersionProvider;
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
public final class Launcher
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

    private Extensions getExtensions()
    {
        ImmutableList<Module> extensionModules = ServiceLoader.load(LauncherPlugin.class).stream()
                .map(ServiceLoader.Provider::get)
                .map(LauncherPlugin::getExtensions)
                .map(Extensions::getAdditionalEnvironments)
                .collect(toImmutableList());
        // TODO workaround because of https://starburstdata.atlassian.net/browse/PRESTO-4502
        //noinspection UnstableApiUsage
        return new Extensions(combine(Iterables.concat(extensionModules, ImmutableList.of(b -> {}))));
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
                    EnvironmentRun.class,
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
            return new String[] {spec.name() + " " + readProjectVersion()};
        }
    }

    private static class LauncherBundle
            extends ListResourceBundle
    {
        @Override
        protected Object[][] getContents()
        {
            File rootDir = new File("");
            if (!new File(rootDir, "mvnw").isFile()) {
                // can be run from non-root directory (eg when started from IntelliJ); try parent
                rootDir = rootDir.getAbsoluteFile().getParentFile();
                checkState(new File(rootDir, "mvnw").isFile(), "Unable to detect project root directory");
            }
            return new Object[][] {
                    {"project.version", readProjectVersion()},
                    {"rootdir", rootDir.getPath()},
                    {"product-tests.module", findModuleName(rootDir, ".*-product-tests")},
                    {"server.module", findModuleName(rootDir, ".*presto-server")},
                    {"server.name", "presto-server"},
                    {"launcher.bin", findModuleName(rootDir, ".*-product-tests-launcher") + "/bin/run-launcher"},
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

    private static String findModuleName(File rootDir, String pattern)
    {
        //noinspection UnstableApiUsage
        File[] matches = requireNonNull(rootDir.listFiles(new PatternFilenameFilter(pattern)));
        if (matches.length == 0) {
            throw new IllegalStateException("No module matches " + pattern);
        }
        if (matches.length > 1) {
            throw new IllegalStateException("More than one module matches pattern " + pattern);
        }
        return matches[0].getName();
    }
}
