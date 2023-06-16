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

import com.github.dockerjava.api.model.Bind;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Module;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.trino.tests.product.launcher.Extensions;
import io.trino.tests.product.launcher.cli.EnvironmentUp.EnvironmentUpOptions;
import io.trino.tests.product.launcher.docker.DockerFiles;
import io.trino.tests.product.launcher.env.DockerContainer;
import io.trino.tests.product.launcher.env.Environment;
import io.trino.tests.product.launcher.env.EnvironmentConfig;
import io.trino.tests.product.launcher.env.EnvironmentFactory;
import io.trino.tests.product.launcher.env.EnvironmentModule;
import io.trino.tests.product.launcher.env.EnvironmentOptions;
import io.trino.tests.product.launcher.util.ConsoleTable;
import org.testcontainers.utility.MountableFile;
import picocli.CommandLine;
import picocli.CommandLine.ExitCode;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.stream.Stream;

import static io.trino.tests.product.launcher.docker.DockerFiles.ROOT_PATH;
import static java.util.Objects.requireNonNull;

@CommandLine.Command(
        name = "describe",
        description = "Describes provided environment",
        usageHelpAutoWidth = true)
public class EnvironmentDescribe
        extends LauncherCommand
{
    private static final Logger log = Logger.get(EnvironmentDescribe.class);

    @Option(names = {"-h", "--help"}, usageHelp = true, description = "Show this help message and exit")
    public boolean usageHelpRequested;

    @Mixin
    public EnvironmentOptions environmentOptions = new EnvironmentOptions();

    @Mixin
    public EnvironmentUpOptions environmentUpOptions = new EnvironmentUpOptions();

    public EnvironmentDescribe(OutputStream outputStream, Extensions extensions)
    {
        super(EnvironmentDescribe.Execution.class, outputStream, extensions);
    }

    @Override
    List<Module> getCommandModules()
    {
        return ImmutableList.of(
                new EnvironmentModule(environmentOptions, extensions.getAdditionalEnvironments()),
                environmentUpOptions.toModule());
    }

    public static class Execution
            implements Callable<Integer>
    {
        private static final String[] CONTAINERS_LIST_HEADER = {
                "container",
                "image",
                "network alias",
                "env",
                "ports",
                "run command"
        };

        private static final String[] MOUNTS_LIST_HEADER = {
                "container",
                "type",
                "from",
                "to",
                "type",
                "size"
        };

        private static final Joiner JOINER = Joiner.on('\n').skipNulls();

        private final EnvironmentFactory environmentFactory;
        private final EnvironmentConfig environmentConfig;
        private final EnvironmentOptions environmentOptions;
        private final EnvironmentUpOptions environmentUpOptions;
        private final Path dockerFilesBasePath;
        private final PrintStream printStream;

        @Inject
        public Execution(
                DockerFiles dockerFiles,
                EnvironmentFactory environmentFactory,
                EnvironmentConfig environmentConfig,
                EnvironmentOptions environmentOptions,
                EnvironmentUpOptions environmentUpOptions,
                PrintStream printStream)
        {
            this.dockerFilesBasePath = dockerFiles.getDockerFilesHostPath();
            this.environmentFactory = requireNonNull(environmentFactory, "environmentFactory is null");
            this.environmentConfig = requireNonNull(environmentConfig, "environmentConfig is null");
            this.environmentOptions = requireNonNull(environmentOptions, "environmentOptions is null");
            this.environmentUpOptions = requireNonNull(environmentUpOptions, "environmentUpOptions is null");
            this.printStream = requireNonNull(printStream, "printStream is null");
        }

        @Override
        public Integer call()
                throws Exception
        {
            Optional<Path> environmentLogPath = environmentUpOptions.logsDirBase.map(dir -> dir.resolve(environmentUpOptions.environment));

            Environment.Builder builder = environmentFactory.get(environmentUpOptions.environment, printStream, environmentConfig, environmentUpOptions.extraOptions)
                    .setContainerOutputMode(environmentOptions.output)
                    .setLogsBaseDir(environmentLogPath);

            Environment environment = builder.build();
            Collection<DockerContainer> containers = environment.getContainers();

            ConsoleTable containersTable = new ConsoleTable();
            containersTable.addHeader(CONTAINERS_LIST_HEADER);

            for (DockerContainer container : containers) {
                containersTable.addRow(
                        container.getLogicalName(),
                        container.getDockerImageName(),
                        JOINER.join(container.getNetworkAliases()),
                        JOINER.join(container.getEnv()),
                        JOINER.join(container.getExposedPorts()),
                        Joiner.on(' ').join(container.getCommandParts()));
                containersTable.addSeparator();
            }

            printStream.printf("Environment '%s' containers:\n%s\n", environmentUpOptions.environment, containersTable.render());

            ConsoleTable mountsTable = new ConsoleTable();
            mountsTable.addHeader(MOUNTS_LIST_HEADER);

            for (DockerContainer container : containers) {
                for (Map.Entry<MountableFile, String> file : container.getCopyToFileContainerPathMap().entrySet()) {
                    MountableFile mountableFile = file.getKey();
                    Path mountedFilePath = Paths.get(mountableFile.getFilesystemPath());
                    boolean isDirectory = Files.isDirectory(mountedFilePath);

                    mountsTable.addRow(
                            container.getLogicalName(),
                            "copy",
                            simplifyPath(mountableFile.getDescription()),
                            file.getValue(),
                            isDirectory ? "dir" : "file",
                            DataSize.ofBytes(isDirectory ? directorySize(mountedFilePath) : mountableFile.getSize()).succinct());
                }

                for (Bind bind : container.getBinds()) {
                    Path path = Paths.get(bind.getPath());
                    boolean isDirectory = Files.isDirectory(path);
                    mountsTable.addRow(
                            container.getLogicalName(),
                            "bind",
                            simplifyPath(bind.getPath()),
                            bind.getVolume().getPath(),
                            isDirectory ? "dir" : "file",
                            DataSize.ofBytes(Files.size(path)).succinct());
                }

                mountsTable.addSeparator();
            }

            printStream.printf("Environment '%s' file mounts:\n%s\n", environmentUpOptions.environment, mountsTable.render());

            return ExitCode.OK;
        }

        private String simplifyPath(String path)
        {
            return path.replace(dockerFilesBasePath.toString(), "classpath:" + ROOT_PATH.substring(0, ROOT_PATH.length() - 1));
        }
    }

    private static long directorySize(Path directory)
    {
        try {
            try (Stream<Path> stream = Files.walk(directory)) {
                return stream
                        .filter(path -> path.toFile().isFile())
                        .mapToLong(path -> path.toFile().length())
                        .sum();
            }
        }
        catch (IOException e) {
            log.warn(e, "Could not calculate directory size: %s", directory);
            return 0;
        }
    }
}
