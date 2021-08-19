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
package io.trino.cli;

import com.google.common.net.HostAndPort;
import io.airlift.units.Duration;
import io.trino.cli.ClientOptions.ClientExtraCredential;
import io.trino.cli.ClientOptions.ClientResourceEstimate;
import io.trino.cli.ClientOptions.ClientSessionProperty;
import picocli.CommandLine;
import picocli.CommandLine.IVersionProvider;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.StandardSystemProperty.USER_HOME;
import static com.google.common.base.Strings.emptyToNull;
import static java.lang.System.getenv;

public final class Trino
{
    private Trino() {}

    public static void main(String[] args)
    {
        System.exit(createCommandLine(new Console()).execute(args));
    }

    public static CommandLine createCommandLine(Object command)
    {
        CommandLine commandLine = new CommandLine(command)
                .setCaseInsensitiveEnumValuesAllowed(true)
                .registerConverter(ClientResourceEstimate.class, ClientResourceEstimate::new)
                .registerConverter(ClientSessionProperty.class, ClientSessionProperty::new)
                .registerConverter(ClientExtraCredential.class, ClientExtraCredential::new)
                .registerConverter(HostAndPort.class, HostAndPort::fromString)
                .registerConverter(Duration.class, Duration::valueOf);

        getConfigFile().ifPresent(file -> ValidatingPropertiesDefaultProvider.attach(commandLine, file));
        return commandLine;
    }

    private static Optional<File> getConfigFile()
    {
        return getConfigSearchPaths()
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(Paths::get)
                .filter(Files::exists)
                .findFirst()
                .map(Path::toFile);
    }

    private static Stream<Optional<String>> getConfigSearchPaths()
    {
        return Stream.of(
                Optional.ofNullable(emptyToNull(getenv("TRINO_CONFIG"))),
                resolveConfigPath(USER_HOME.value(), ".trino_config"),
                resolveConfigPath(getenv("XDG_CONFIG_HOME"), "/trino/config"));
    }

    private static Optional<String> resolveConfigPath(String root, String file)
    {
        return Optional.ofNullable(emptyToNull(root))
                .map(Paths::get)
                .filter(Files::exists)
                .map(path -> path.resolve(file).toString());
    }

    public static class VersionProvider
            implements IVersionProvider
    {
        @Override
        public String[] getVersion()
        {
            String version = getClass().getPackage().getImplementationVersion();
            return new String[] {"Trino CLI " + firstNonNull(version, "(version unknown)")};
        }
    }
}
