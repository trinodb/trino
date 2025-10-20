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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Streams.stream;
import static io.trino.util.Executors.executeUntilFailure;
import static java.nio.file.Files.newDirectoryStream;
import static java.util.Objects.requireNonNull;

public class ServerPluginsProvider
        implements PluginsProvider
{
    private final List<Path> installedPluginsDirs;
    private final Executor executor;

    @Inject
    public ServerPluginsProvider(ServerPluginsProviderConfig config, @ForStartup Executor executor)
    {
        this.installedPluginsDirs = config.getInstalledPluginsDirs();
        this.executor = requireNonNull(executor, "executor is null");
    }

    @Override
    public void loadPlugins(Loader loader, ClassLoaderFactory createClassLoader)
    {
        executeUntilFailure(
                executor,
                installedPluginsDirs.stream()
                        .flatMap(installedPluginsDir -> listFiles(installedPluginsDir).stream())
                        .filter(Files::isDirectory)
                        .map(Path::toAbsolutePath)
                        .map(path -> (Callable<?>) () -> {
                            String name = path.getFileName().toString();
                            loader.load(name, () -> createClassLoader.create(name, buildClassPath(path)));
                            return null;
                        })
                        .collect(toImmutableList()));
    }

    private static List<URL> buildClassPath(Path path)
    {
        return listFiles(path).stream()
                .map(ServerPluginsProvider::fileToUrl)
                .collect(toImmutableList());
    }

    private static List<Path> listFiles(Path path)
    {
        try {
            try (DirectoryStream<Path> directoryStream = newDirectoryStream(path)) {
                return stream(directoryStream)
                        .sorted()
                        .collect(toImmutableList());
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static URL fileToUrl(Path file)
    {
        try {
            return file.toUri().toURL();
        }
        catch (MalformedURLException e) {
            throw new UncheckedIOException(e);
        }
    }
}
