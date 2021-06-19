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

import com.google.common.base.CharMatcher;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import picocli.CommandLine;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;

public class ValidatingPropertiesDefaultProvider
        extends CommandLine.PropertiesDefaultProvider
{
    protected ValidatingPropertiesDefaultProvider(File configFile)
    {
        super(configFile);
    }

    public static void attach(CommandLine commandLine, File configFile)
    {
        validateConfigurationProperties(commandLine.getCommandSpec(), configFile.getPath());
        commandLine.setDefaultValueProvider(new ValidatingPropertiesDefaultProvider(configFile));
    }

    private static void validateConfigurationProperties(CommandLine.Model.CommandSpec commandSpec, String path)
    {
        CharMatcher isDash = CharMatcher.is('-');

        try (InputStream inputStream = Files.newInputStream(Paths.get(path))) {
            Properties properties = new Properties();
            properties.load(inputStream);
            Set<String> definedOptions = properties.stringPropertyNames();

            Set<String> knownOptions = commandSpec.args().stream()
                    .flatMap(ValidatingPropertiesDefaultProvider::getOptionNamesFromSpec)
                    .map(isDash::trimLeadingFrom)
                    .collect(toImmutableSet());

            Sets.SetView<String> unknownOptions = Sets.difference(definedOptions, knownOptions);

            if (!unknownOptions.isEmpty()) {
                System.err.printf("Configuration file %s contains unknown properties %s\n", path, unknownOptions);
                System.exit(1);
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static Stream<String> getOptionNames(CommandLine.Model.OptionSpec optionSpec)
    {
        if (optionSpec.usageHelp() || optionSpec.versionHelp()) {
            return Stream.empty();
        }
        return ImmutableList.copyOf(optionSpec.names()).stream();
    }

    private static Stream<String> getOptionNamesFromSpec(CommandLine.Model.ArgSpec argSpec)
    {
        if (argSpec instanceof CommandLine.Model.OptionSpec) {
            return getOptionNames((CommandLine.Model.OptionSpec) argSpec);
        }

        if (argSpec instanceof CommandLine.Model.PositionalParamSpec) {
            return getOptionNames((CommandLine.Model.PositionalParamSpec) argSpec);
        }

        throw new RuntimeException("Unknown option " + argSpec);
    }

    private static Stream<String> getOptionNames(CommandLine.Model.PositionalParamSpec paramSpec)
    {
        return ImmutableList.of(paramSpec.paramLabel()).stream();
    }
}
