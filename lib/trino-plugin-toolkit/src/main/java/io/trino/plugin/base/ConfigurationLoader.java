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
package io.trino.plugin.base;

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.TomlConfiguration;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;

import static io.airlift.configuration.ConfigurationLoader.loadPropertiesFrom;
import static io.airlift.configuration.TomlConfiguration.createTomlConfiguration;

public class ConfigurationLoader
{
    public static final String PROPERTIES_SUFFIX = ".properties";
    public static final String TOML_SUFFIX = ".toml";

    private ConfigurationLoader() {}

    public static boolean configurationExists(Path path)
    {
        return resolveConfigurationPath(path).isPresent();
    }

    public static boolean isConfigurationFile(File file)
    {
        return file.getAbsolutePath().endsWith(PROPERTIES_SUFFIX) || file.getAbsolutePath().endsWith(TOML_SUFFIX);
    }

    public static Map<String, String> loadConfigurationFrom(String path)
            throws IOException
    {
        return loadConfigurationFrom(Paths.get(path));
    }

    public static Map<String, String> loadConfigurationFrom(Path path)
            throws IOException
    {
        return loadConfigurationFrom(path.toAbsolutePath().toFile());
    }

    public static Map<String, String> loadConfigurationFrom(File file)
            throws IOException
    {
        Optional<Path> configurationPath = resolveConfigurationPath(file.toPath());
        if (configurationPath.isEmpty()) {
            throw new IOException("Cannot resolve configuration from path: " + file);
        }
        return internalLoadConfiguration(configurationPath.get());
    }

    public static Path resolvedConfigurationPath(Path path)
    {
        return resolveConfigurationPath(path)
                .orElseThrow(() -> new IllegalArgumentException("Cannot resolve configuration from path: " + path));
    }

    private static Optional<Path> resolveConfigurationPath(Path path)
    {
        if (Files.exists(path)) {
            return Optional.of(path.toAbsolutePath());
        }

        Path tomlPath = Path.of(path.toFile().getPath() + TOML_SUFFIX);
        if (Files.exists(tomlPath)) {
            return Optional.of(tomlPath.toAbsolutePath());
        }

        Path propertiesPath = Path.of(path.toFile().getPath() + PROPERTIES_SUFFIX);
        if (Files.exists(propertiesPath)) {
            return Optional.of(propertiesPath.toAbsolutePath());
        }

        // For backward compatibility assume that the configuration is a properties file
        if (Files.exists(path)) {
            return Optional.of(path.toAbsolutePath());
        }

        return Optional.empty();
    }

    private static Map<String, String> internalLoadConfiguration(Path path)
            throws IOException
    {
        if (path.toFile().getPath().endsWith(TOML_SUFFIX) && Files.exists(path)) {
            return loadTomlConfiguration(path);
        }

        // For backward compatibility assume that the configuration is a properties file
        if (Files.exists(path)) {
            return loadPropertiesFrom(path.toFile().getPath());
        }

        throw new IOException("Configuration file does not exist: " + path);
    }

    private static Map<String, String> loadTomlConfiguration(Path path)
    {
        TomlConfiguration tomlConfiguration = createTomlConfiguration(path.toFile());
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();

        for (String namespace : tomlConfiguration.getNamespaces()) {
            tomlConfiguration.getNamespaceConfiguration(namespace)
                    .forEach((key, value) -> builder.put(namespace + "." + key, value));
        }

        tomlConfiguration.getParentConfiguration().forEach(builder::put);
        return builder.buildOrThrow();
    }
}
