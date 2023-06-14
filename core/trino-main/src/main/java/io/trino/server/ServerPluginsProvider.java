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
package io.trino.server;

import com.google.inject.Inject;
import io.trino.server.PluginManager.PluginsProvider;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.difference;
import static com.google.common.collect.Streams.stream;
import static io.trino.util.Executors.executeUntilFailure;
import static java.nio.file.Files.newDirectoryStream;
import static java.util.Objects.requireNonNull;

public class ServerPluginsProvider
        implements PluginsProvider
{
    private final File installedPluginsDir;
    private final Executor executor;
    private final Set<String> disabledPlugins;

    @Inject
    public ServerPluginsProvider(ServerPluginsProviderConfig config, @ForStartup Executor executor)
    {
        requireNonNull(config, "config is null");
        this.installedPluginsDir = config.getInstalledPluginsDir();
        this.disabledPlugins = config.getDisabledPlugins();
        this.executor = requireNonNull(executor, "executor is null");
    }

    @Override
    public void loadPlugins(Loader loader, ClassLoaderFactory createClassLoader)
    {
        List<File> plugins = listFiles(installedPluginsDir).stream()
                .filter(File::isDirectory)
                .collect(toImmutableList());
        Set<String> pluginNames = plugins.stream().map(File::getName).collect(toImmutableSet());
        if (!pluginNames.containsAll(disabledPlugins)) {
            throw new IllegalArgumentException("disabled plugins configuration property contains not existing plugin names: %s. disabledPlugins: %s, pluginNames: %s"
                    .formatted(difference(disabledPlugins, pluginNames), disabledPlugins, pluginNames));
        }
        executeUntilFailure(
                executor,
                plugins.stream()
                        .filter(plugin -> !disabledPlugins.contains(plugin.getName()))
                        .map(file -> (Callable<?>) () -> {
                            loader.load(file.getAbsolutePath(), () ->
                                    createClassLoader.create(file.getName(), buildClassPath(file)));
                            return null;
                        })
                        .collect(toImmutableList()));
    }

    private static List<URL> buildClassPath(File path)
    {
        return listFiles(path).stream()
                .map(ServerPluginsProvider::fileToUrl)
                .collect(toImmutableList());
    }

    private static List<File> listFiles(File path)
    {
        try {
            try (DirectoryStream<Path> directoryStream = newDirectoryStream(path.toPath())) {
                return stream(directoryStream)
                        .map(Path::toFile)
                        .sorted()
                        .collect(toImmutableList());
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static URL fileToUrl(File file)
    {
        try {
            return file.toURI().toURL();
        }
        catch (MalformedURLException e) {
            throw new UncheckedIOException(e);
        }
    }
}
