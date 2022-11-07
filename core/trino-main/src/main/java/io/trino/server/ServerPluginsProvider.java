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

import io.trino.server.PluginManager.PluginsProvider;

import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.DirectoryStream;
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
    private final File installedPluginsDir;
    private final Executor executor;

    @Inject
    public ServerPluginsProvider(ServerPluginsProviderConfig config, @ForStartup Executor executor)
    {
        this.installedPluginsDir = config.getInstalledPluginsDir();
        this.executor = requireNonNull(executor, "executor is null");
    }

    @Override
    public void loadPlugins(Loader loader, ClassLoaderFactory createClassLoader)
    {
        executeUntilFailure(
                executor,
                listFiles(installedPluginsDir).stream()
                        .filter(File::isDirectory)
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
