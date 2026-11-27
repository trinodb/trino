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
package io.trino.hdfs.nativefs;

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.memory.MemoryFileSystemFactory;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.security.ConnectorIdentity;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestNativeHadoopFileSystem
{
    private String registryId;
    private TrinoFileSystem nativeFileSystem;
    private Configuration configuration;
    private FileSystem fileSystem;

    @BeforeEach
    void setUp()
            throws IOException
    {
        registryId = "test-" + UUID.randomUUID();
        MemoryFileSystemFactory factory = new MemoryFileSystemFactory();
        nativeFileSystem = factory.create(ConnectorIdentity.ofUser("test"));
        NativeFileSystemRegistry.register(registryId, Map.of("memory", factory));

        configuration = new Configuration(false);
        new NativeFileSystemConfigurationInitializer(registryId).initializeConfiguration(configuration);

        fileSystem = new NativeHadoopFileSystem();
        fileSystem.initialize(URI.create("memory:///"), configuration);
    }

    @AfterEach
    void tearDown()
    {
        NativeFileSystemRegistry.unregister(registryId);
    }

    @Test
    void testConfigurationInitializer()
    {
        assertThat(configuration.get("fs.memory.impl")).isEqualTo(NativeHadoopFileSystem.class.getName());
        assertThat(configuration.get(NativeHadoopFileSystem.REGISTRY_ID_KEY)).isEqualTo(registryId);
        // Only schemes with a registered native file system are overridden
        assertThat(configuration.get("fs.s3a.impl")).isNull();
    }

    @Test
    void testConfigurationInitializerKeepsConfiguredFileSystem()
    {
        Configuration config = new Configuration(false);
        // A Hadoop file system the user provided is kept
        config.set("fs.memory.impl", "org.apache.hadoop.fs.RawLocalFileSystem");
        new NativeFileSystemConfigurationInitializer(registryId).initializeConfiguration(config);
        assertThat(config.get("fs.memory.impl")).isEqualTo("org.apache.hadoop.fs.RawLocalFileSystem");

        // A Hadoop file system that is not available, such as the S3A default, is replaced
        config.set("fs.memory.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        new NativeFileSystemConfigurationInitializer(registryId).initializeConfiguration(config);
        assertThat(config.get("fs.memory.impl")).isEqualTo(NativeHadoopFileSystem.class.getName());
    }

    @Test
    void testInitializeWithUnregisteredScheme()
    {
        assertThatThrownBy(() -> new NativeHadoopFileSystem().initialize(URI.create("s3a://bucket/"), configuration))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("No native file system registered for scheme: s3a");
    }

    @Test
    void testInitializeWithoutRegistryId()
    {
        assertThatThrownBy(() -> new NativeHadoopFileSystem().initialize(URI.create("memory:///"), new Configuration(false)))
                .isInstanceOf(IOException.class)
                .hasMessageContaining(NativeHadoopFileSystem.REGISTRY_ID_KEY);
    }

    @Test
    void testOpen()
            throws IOException
    {
        writeNative("memory:///data/file.txt", "hello world");

        try (FSDataInputStream input = fileSystem.open(new Path("memory:///data/file.txt"))) {
            assertThat(new String(input.readAllBytes(), UTF_8)).isEqualTo("hello world");
        }
        try (FSDataInputStream input = fileSystem.open(new Path("memory:///data/file.txt"))) {
            input.seek(6);
            assertThat(input.getPos()).isEqualTo(6);
            assertThat(new String(input.readAllBytes(), UTF_8)).isEqualTo("world");
            // positioned reads do not move the stream position
            byte[] buffer = new byte[5];
            input.readFully(0, buffer);
            assertThat(new String(buffer, UTF_8)).isEqualTo("hello");
            assertThat(input.getPos()).isEqualTo(11);
        }
    }

    @Test
    void testOpenMissingFile()
    {
        assertThatThrownBy(() -> {
            try (FSDataInputStream input = fileSystem.open(new Path("memory:///data/missing.txt"))) {
                input.read();
            }
        }).isInstanceOf(FileNotFoundException.class);
    }

    @Test
    void testGetFileStatus()
            throws IOException
    {
        writeNative("memory:///data/file.txt", "hello");

        FileStatus file = fileSystem.getFileStatus(new Path("memory:///data/file.txt"));
        assertThat(file.isFile()).isTrue();
        assertThat(file.getLen()).isEqualTo(5);
        assertThat(file.getPath().getName()).isEqualTo("file.txt");

        FileStatus directory = fileSystem.getFileStatus(new Path("memory:///data"));
        assertThat(directory.isDirectory()).isTrue();

        assertThat(fileSystem.getFileStatus(new Path("memory:///")).isDirectory()).isTrue();

        assertThatThrownBy(() -> fileSystem.getFileStatus(new Path("memory:///missing")))
                .isInstanceOf(FileNotFoundException.class);
    }

    @Test
    void testListStatus()
            throws IOException
    {
        writeNative("memory:///data/a.txt", "a");
        writeNative("memory:///data/b.txt", "bb");
        writeNative("memory:///data/sub/c.txt", "ccc");

        // direct children only: files and subdirectories, but not files in subdirectories
        FileStatus[] statuses = fileSystem.listStatus(new Path("memory:///data"));
        assertThat(Arrays.stream(statuses).filter(FileStatus::isFile).map(status -> status.getPath().getName()))
                .containsExactlyInAnyOrder("a.txt", "b.txt");
        assertThat(Arrays.stream(statuses).filter(FileStatus::isDirectory).map(status -> status.getPath().getName()))
                .containsExactly("sub");

        // listing a file returns the file itself
        assertThat(fileSystem.listStatus(new Path("memory:///data/b.txt")))
                .singleElement()
                .satisfies(status -> assertThat(status.getLen()).isEqualTo(2));

        // Object stores cannot distinguish a missing directory from an empty one, such as a "dir/" marker
        assertThat(fileSystem.listStatus(new Path("memory:///missing"))).isEmpty();
    }

    @Test
    void testListFilesRecursive()
            throws IOException
    {
        writeNative("memory:///data/a.txt", "a");
        writeNative("memory:///data/sub/c.txt", "ccc");

        assertThat(listFiles(fileSystem, new Path("memory:///data")))
                .containsExactlyInAnyOrder("a.txt", "c.txt");
    }

    @Test
    void testCreate()
            throws IOException
    {
        Path path = new Path("memory:///data/created.txt");
        try (FSDataOutputStream output = fileSystem.create(path, false)) {
            output.write("first".getBytes(UTF_8));
        }
        assertThat(readNative("memory:///data/created.txt")).isEqualTo("first");

        assertThatThrownBy(() -> fileSystem.create(path, false))
                .isInstanceOf(FileAlreadyExistsException.class)
                .hasMessageContaining("File already exists");

        try (FSDataOutputStream output = fileSystem.create(path, true)) {
            output.write("second".getBytes(UTF_8));
        }
        assertThat(readNative("memory:///data/created.txt")).isEqualTo("second");
    }

    @Test
    void testAppend()
    {
        assertThatThrownBy(() -> fileSystem.append(new Path("memory:///data/file.txt")))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Append is not supported");
    }

    @Test
    void testRename()
            throws IOException
    {
        writeNative("memory:///data/source.txt", "content");

        assertThat(fileSystem.rename(new Path("memory:///data/source.txt"), new Path("memory:///data/target.txt"))).isTrue();

        assertThat(nativeFileSystem.newInputFile(Location.of("memory:///data/source.txt")).exists()).isFalse();
        assertThat(readNative("memory:///data/target.txt")).isEqualTo("content");

        // Hadoop contract: renaming a missing path returns false
        assertThat(fileSystem.rename(new Path("memory:///data/missing.txt"), new Path("memory:///data/other.txt"))).isFalse();
    }

    @Test
    void testDelete()
            throws IOException
    {
        writeNative("memory:///data/a.txt", "a");
        writeNative("memory:///data/sub/b.txt", "b");

        assertThat(fileSystem.delete(new Path("memory:///data/a.txt"), false)).isTrue();
        assertThat(nativeFileSystem.newInputFile(Location.of("memory:///data/a.txt")).exists()).isFalse();

        assertThatThrownBy(() -> fileSystem.delete(new Path("memory:///data"), false))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Directory is not empty");

        assertThat(fileSystem.delete(new Path("memory:///data"), true)).isTrue();
        assertThat(nativeFileSystem.newInputFile(Location.of("memory:///data/sub/b.txt")).exists()).isFalse();

        // Hadoop contract: deleting a missing path returns false
        assertThat(fileSystem.delete(new Path("memory:///data"), true)).isFalse();
        assertThat(fileSystem.delete(new Path("memory:///missing.txt"), false)).isFalse();

        assertThatThrownBy(() -> fileSystem.delete(new Path("memory:///"), true))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Cannot delete the root of the file system");
    }

    @Test
    void testViewFsMountLink()
            throws IOException
    {
        // ViewFS resolves the mount link target by scheme, using fs.memory.impl set by the initializer
        writeNative("memory:///mounted/a.txt", "a");
        writeNative("memory:///mounted/sub/b.txt", "bb");

        Configuration viewFsConfiguration = new Configuration(configuration);
        viewFsConfiguration.set("fs.viewfs.impl", "org.apache.hadoop.fs.viewfs.ViewFileSystem");
        viewFsConfiguration.set("fs.viewfs.mounttable.cluster.link./data", "memory:///mounted");

        FileSystem viewFs = FileSystem.newInstance(URI.create("viewfs://cluster/"), viewFsConfiguration);
        Path data = new Path("viewfs://cluster/data");

        assertThat(Arrays.stream(viewFs.listStatus(data)).map(status -> status.getPath().getName()))
                .containsExactlyInAnyOrder("a.txt", "sub");
        assertThat(listFiles(viewFs, data)).containsExactlyInAnyOrder("a.txt", "b.txt");

        try (FSDataInputStream input = viewFs.open(new Path("viewfs://cluster/data/sub/b.txt"))) {
            assertThat(new String(input.readAllBytes(), UTF_8)).isEqualTo("bb");
        }
    }

    @Test
    void testNativeCallsUseNativeContextClassLoader()
            throws Exception
    {
        // Record the context class loader of every call into the native file system
        MemoryFileSystemFactory memory = new MemoryFileSystemFactory();
        List<ClassLoader> contextClassLoaders = new CopyOnWriteArrayList<>();
        TrinoFileSystemFactory recordingFactory = identity -> {
            contextClassLoaders.add(Thread.currentThread().getContextClassLoader());
            TrinoFileSystem delegate = memory.create(identity);
            return (TrinoFileSystem) Proxy.newProxyInstance(
                    TrinoFileSystem.class.getClassLoader(),
                    new Class<?>[] {TrinoFileSystem.class},
                    (_, method, arguments) -> {
                        contextClassLoaders.add(Thread.currentThread().getContextClassLoader());
                        try {
                            return method.invoke(delegate, arguments);
                        }
                        catch (InvocationTargetException e) {
                            throw e.getCause();
                        }
                    });
        };
        String recordingRegistryId = "test-" + UUID.randomUUID();
        NativeFileSystemRegistry.register(recordingRegistryId, Map.of("memory", recordingFactory));
        Configuration recordingConfiguration = new Configuration(false);
        new NativeFileSystemConfigurationInitializer(recordingRegistryId).initializeConfiguration(recordingConfiguration);

        // Hadoop calls the file system with an unrelated context class loader, such as the HDFS class loader
        try (URLClassLoader unrelatedClassLoader = new URLClassLoader(new URL[0], null);
                ThreadContextClassLoader _ = new ThreadContextClassLoader(unrelatedClassLoader)) {
            FileSystem recordingFileSystem = new NativeHadoopFileSystem();
            recordingFileSystem.initialize(URI.create("memory:///"), recordingConfiguration);
            try (FSDataOutputStream output = recordingFileSystem.create(new Path("memory:///data/file.txt"), true)) {
                output.write("content".getBytes(UTF_8));
            }
            assertThat(recordingFileSystem.listStatus(new Path("memory:///data"))).hasSize(1);
            assertThat(recordingFileSystem.getFileStatus(new Path("memory:///data/file.txt")).getLen()).isEqualTo(7);
            try (FSDataInputStream input = recordingFileSystem.open(new Path("memory:///data/file.txt"))) {
                assertThat(new String(input.readAllBytes(), UTF_8)).isEqualTo("content");
            }
            assertThat(recordingFileSystem.delete(new Path("memory:///data"), true)).isTrue();
        }
        finally {
            NativeFileSystemRegistry.unregister(recordingRegistryId);
        }

        assertThat(contextClassLoaders)
                .isNotEmpty()
                .allMatch(classLoader -> classLoader == recordingFactory.getClass().getClassLoader());
    }

    private static List<String> listFiles(FileSystem fileSystem, Path path)
            throws IOException
    {
        List<String> names = new ArrayList<>();
        RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(path, true);
        while (files.hasNext()) {
            names.add(files.next().getPath().getName());
        }
        return names;
    }

    private void writeNative(String location, String content)
            throws IOException
    {
        nativeFileSystem.newOutputFile(Location.of(location)).createOrOverwrite(content.getBytes(UTF_8));
    }

    private String readNative(String location)
            throws IOException
    {
        try (var input = nativeFileSystem.newInputFile(Location.of(location)).newStream()) {
            return new String(input.readAllBytes(), UTF_8);
        }
    }
}
