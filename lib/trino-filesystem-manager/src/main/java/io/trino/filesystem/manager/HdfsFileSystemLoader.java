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
package io.trino.filesystem.manager;

import io.opentelemetry.api.OpenTelemetry;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.spi.NodeManager;
import io.trino.spi.Plugin;
import io.trino.spi.classloader.ThreadContextClassLoader;
import jakarta.annotation.PreDestroy;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Streams.stream;
import static java.nio.file.Files.newDirectoryStream;

final class HdfsFileSystemLoader
{
    private final HdfsClassLoader classLoader;
    private final Object manager;

    public HdfsFileSystemLoader(
            Map<String, String> config,
            boolean azureEnabled,
            boolean gcsEnabled,
            boolean s3Enabled,
            String catalogName,
            NodeManager nodeManager,
            OpenTelemetry openTelemetry)
    {
        Class<?> clazz = tryLoadExistingHdfsManager();

        // check if we are running inside a plugin class loader (full server mode)
        if (!getClass().getClassLoader().equals(Plugin.class.getClassLoader())) {
            verify(clazz == null, "HDFS should not be on the plugin classpath");
            File sourceFile = getCurrentClassLocation();
            File directory;
            if (sourceFile.isDirectory()) {
                // running DevelopmentServer in the IDE
                verify(sourceFile.getPath().endsWith("/target/classes"), "Source file not in 'target' directory: %s", sourceFile);
                directory = new File(sourceFile.getParentFile().getParentFile().getParentFile(), "trino-hdfs/target/hdfs");
            }
            else {
                // normal server mode where HDFS JARs are in a subdirectory of the plugin
                directory = new File(sourceFile.getParentFile(), "hdfs");
            }
            verify(directory.isDirectory(), "HDFS directory is missing: %s", directory);
            classLoader = createClassLoader(directory);
            clazz = loadHdfsManager(classLoader);
        }
        else {
            verify(clazz != null, "HDFS should be on the classpath for tests");
            classLoader = null;
        }

        try (var ignored = new ThreadContextClassLoader(classLoader)) {
            manager = clazz.getConstructor(Map.class, boolean.class, boolean.class, boolean.class, String.class, NodeManager.class, OpenTelemetry.class)
                    .newInstance(config, azureEnabled, gcsEnabled, s3Enabled, catalogName, nodeManager, openTelemetry);
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    public Set<String> configure()
    {
        try (var ignored = new ThreadContextClassLoader(classLoader)) {
            return (Set<String>) manager.getClass().getMethod("configure").invoke(manager);
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException("Failed to configure HDFS:\n%s\n%s\n%s".formatted("<".repeat(70), e.getCause(), ">".repeat(70)), e);
        }
    }

    public TrinoFileSystemFactory create()
    {
        try (var ignored = new ThreadContextClassLoader(classLoader)) {
            return (TrinoFileSystemFactory) manager.getClass().getMethod("create").invoke(manager);
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(e);
        }
    }

    @PreDestroy
    public void stop()
            throws IOException, ReflectiveOperationException
    {
        try (classLoader; var ignored = new ThreadContextClassLoader(classLoader)) {
            manager.getClass().getMethod("stop").invoke(manager);
        }
    }

    private File getCurrentClassLocation()
    {
        try {
            return new File(getClass().getProtectionDomain().getCodeSource().getLocation().toURI());
        }
        catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private Class<?> tryLoadExistingHdfsManager()
    {
        try {
            return loadHdfsManager(getClass().getClassLoader());
        }
        catch (RuntimeException e) {
            return null;
        }
    }

    private static Class<?> loadHdfsManager(ClassLoader classLoader)
    {
        try {
            return classLoader.loadClass("io.trino.filesystem.hdfs.HdfsFileSystemManager");
        }
        catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private static HdfsClassLoader createClassLoader(File path)
    {
        List<URL> urls = buildClassPath(path);
        verify(!urls.isEmpty(), "HDFS directory is empty: %s", path);
        return new HdfsClassLoader(urls);
    }

    private static List<URL> buildClassPath(File path)
    {
        try (DirectoryStream<Path> directoryStream = newDirectoryStream(path.toPath())) {
            return stream(directoryStream)
                    .map(Path::toFile)
                    .sorted().toList().stream()
                    .map(HdfsFileSystemLoader::fileToUrl)
                    .toList();
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
