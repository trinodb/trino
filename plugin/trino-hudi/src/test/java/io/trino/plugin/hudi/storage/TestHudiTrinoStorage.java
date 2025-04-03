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
package io.trino.plugin.hudi.storage;

import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.local.LocalFileSystemFactory;
import io.trino.spi.block.TestingSession;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.io.storage.TestHoodieStorageBase;
import org.apache.hudi.io.util.IOUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Test {@link HudiTrinoStorage}
 */
public class TestHudiTrinoStorage
        extends TestHoodieStorageBase
{
    private static TrinoFileSystem fileSystem;
    private static final byte[] EMPTY_BYTES = new byte[] {};

    @BeforeAll
    static void setUp()
    {
        fileSystem = new LocalFileSystemFactory(Path.of("/tmp"))
                .create(TestingSession.SESSION);
    }

    @Override
    protected HoodieStorage getStorage(Object fs, Object conf)
    {
        return new HudiTrinoStorage((TrinoFileSystem) fs, (TrinoStorageConfiguration) conf);
    }

    @Override
    protected TrinoFileSystem getFileSystem(Object conf)
    {
        return fileSystem;
    }

    @Override
    protected TrinoStorageConfiguration getConf()
    {
        return new TrinoStorageConfiguration();
    }

    @Override
    protected String getTempDir()
    {
        return "local:" + this.tempDir.toUri().getPath();
    }

    @Override
    @Test
    public void testGetUri()
            throws URISyntaxException
    {
        assertThat(getStorage().getUri()).isEqualTo(new URI(""));
    }

    @Override
    @Test
    public void testListing()
            throws IOException
    {
        HoodieStorage storage = getStorage();
        // Full list:
        // w/1.file
        // w/2.file
        // x/1.file
        // x/2.file
        // x/y/1.file
        // x/y/2.file
        // x/z/1.file
        // x/z/2.file
        prepareFilesOnStorage(storage);

        validatePathInfoList(
                Arrays.stream(new StoragePathInfo[] {
                        getStoragePathInfo("x/1.file", false),
                        getStoragePathInfo("x/2.file", false),
                        getStoragePathInfo("x/y", true),
                        getStoragePathInfo("x/z", true)
                }).collect(Collectors.toList()),
                storage.listDirectEntries(new StoragePath(getTempDir(), "x")));

        validatePathInfoList(
                Arrays.stream(new StoragePathInfo[] {
                        getStoragePathInfo("x/1.file", false),
                        getStoragePathInfo("x/2.file", false),
                        getStoragePathInfo("x/y/1.file", false),
                        getStoragePathInfo("x/y/2.file", false),
                        getStoragePathInfo("x/z/1.file", false),
                        getStoragePathInfo("x/z/2.file", false)
                }).collect(Collectors.toList()),
                storage.listFiles(new StoragePath(getTempDir(), "x")));

        validatePathInfoList(
                Arrays.stream(new StoragePathInfo[] {
                        getStoragePathInfo("x/2.file", false)
                }).collect(Collectors.toList()),
                storage.listDirectEntries(
                        new StoragePath(getTempDir(), "x"), e -> e.getName().contains("2")));

        validatePathInfoList(
                Arrays.stream(new StoragePathInfo[] {
                        getStoragePathInfo("w/1.file", false),
                        getStoragePathInfo("w/2.file", false),
                        getStoragePathInfo("x/z/1.file", false),
                        getStoragePathInfo("x/z/2.file", false)
                }).collect(Collectors.toList()),
                storage.listDirectEntries(Arrays.stream(new StoragePath[] {
                        new StoragePath(getTempDir(), "w"),
                        new StoragePath(getTempDir(), "x/z")
                }).collect(Collectors.toList())));

        assertThatThrownBy(
                () -> storage.listDirectEntries(new StoragePath(getTempDir(), "*")))
                .isInstanceOf(FileNotFoundException.class);

        assertThatThrownBy(
                () -> storage.globEntries(new StoragePath(getTempDir(), "x/*/1.file")))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Override
    @Test
    public void testDelete()
            throws IOException
    {
        HoodieStorage storage = getStorage();

        StoragePath path = new StoragePath(getTempDir(), "testDelete/1.file");
        assertThat(storage.exists(path)).isFalse();
        storage.create(path).close();
        assertThat(storage.exists(path)).isTrue();

        assertThat(storage.deleteFile(path)).isTrue();
        assertThat(storage.exists(path)).isFalse();
        // TrinoFileSystem does not indicate whether the file to delete exists or not
        assertThat(storage.deleteFile(path)).isTrue();

        StoragePath path2 = new StoragePath(getTempDir(), "testDelete/2");
        assertThat(storage.exists(path2)).isFalse();
        assertThat(storage.createDirectory(path2)).isTrue();
        assertThat(storage.exists(path2)).isTrue();

        assertThat(storage.deleteDirectory(path2)).isTrue();
        assertThat(storage.exists(path2)).isFalse();
        // TrinoFileSystem does not indicate whether the directory to delete exists or not
        assertThat(storage.deleteDirectory(path2)).isTrue();
    }

    @Override
    @Test
    public void testCreateWriteAndRead()
            throws IOException
    {
        HoodieStorage storage = getStorage();

        StoragePath path = new StoragePath(getTempDir(), "testCreateAppendAndRead/1.file");
        assertThat(storage.exists(path)).isFalse();
        storage.create(path).close();
        validatePathInfo(storage, path, EMPTY_BYTES, false);
        storage.deleteFile(path);

        byte[] data = new byte[] {2, 42, 49, (byte) 158, (byte) 233, 66, 9};

        try (OutputStream stream = storage.create(path)) {
            stream.write(data);
            stream.flush();
        }
        validatePathInfo(storage, path, data, false);

        assertThatThrownBy(() -> storage.create(path, false))
                .isInstanceOf(IOException.class);
        validatePathInfo(storage, path, data, false);

        assertThatThrownBy(() -> storage.create(path, false))
                .isInstanceOf(IOException.class);
        validatePathInfo(storage, path, data, false);

        StoragePath path2 = new StoragePath(getTempDir(), "testCreateAppendAndRead/2.file");
        assertThat(storage.exists(path2)).isFalse();
        assertThat(storage.createNewFile(path2)).isTrue();
        validatePathInfo(storage, path2, EMPTY_BYTES, false);
        assertThat(storage.createNewFile(path2)).isFalse();

        StoragePath path3 = new StoragePath(getTempDir(), "testCreateAppendAndRead/3.file");
        assertThat(storage.exists(path3)).isFalse();
        storage.createImmutableFileInPath(path3, Option.of(data));
        validatePathInfo(storage, path3, data, false);

        StoragePath path4 = new StoragePath(getTempDir(), "testCreateAppendAndRead/4");
        assertThat(storage.exists(path4)).isFalse();
        assertThat(storage.createDirectory(path4)).isTrue();
        validatePathInfo(storage, path4, EMPTY_BYTES, true);
        assertThat(storage.createDirectory(path4)).isTrue();
    }

    private HoodieStorage getStorage()
    {
        Object conf = getConf();
        return getStorage(getFileSystem(conf), conf);
    }

    private StoragePathInfo getStoragePathInfo(String subPath, boolean isDirectory)
    {
        return new StoragePathInfo(new StoragePath(getTempDir(), subPath),
                0, isDirectory, (short) 1, 1000000L, 10L);
    }

    private void validatePathInfo(
            HoodieStorage storage,
            StoragePath path,
            byte[] data,
            boolean isDirectory)
            throws IOException
    {
        assertThat(storage.exists(path)).isTrue();
        StoragePathInfo pathInfo = storage.getPathInfo(path);
        assertThat(pathInfo.getPath()).isEqualTo(path);
        assertThat(pathInfo.isDirectory()).isEqualTo(isDirectory);
        assertThat(pathInfo.isFile()).isEqualTo(!isDirectory);
        if (!isDirectory) {
            assertThat(pathInfo.getLength()).isEqualTo(data.length);
            try (InputStream stream = storage.open(path)) {
                assertThat(IOUtils.readAsByteArray(stream, data.length)).isEqualTo(data);
            }
            assertThat(pathInfo.getModificationTime()).isGreaterThan(0);
        }
    }

    private void validatePathInfoList(
            List<StoragePathInfo> expected,
            List<StoragePathInfo> actual)
    {
        assertThat(actual.size()).isEqualTo(expected.size());
        List<StoragePathInfo> sortedExpected = expected.stream()
                .sorted(Comparator.comparing(StoragePathInfo::getPath))
                .collect(Collectors.toList());
        List<StoragePathInfo> sortedActual = actual.stream()
                .sorted(Comparator.comparing(StoragePathInfo::getPath))
                .collect(Collectors.toList());
        for (int i = 0; i < expected.size(); i++) {
            // We cannot use StoragePathInfo#equals as that only compares the path
            assertThat(sortedActual.get(i).getPath()).isEqualTo(sortedExpected.get(i).getPath());
            assertThat(sortedActual.get(i).isDirectory()).isEqualTo(sortedExpected.get(i).isDirectory());
            assertThat(sortedActual.get(i).isFile()).isEqualTo(sortedExpected.get(i).isFile());
            if (sortedExpected.get(i).isFile()) {
                assertThat(sortedActual.get(i).getLength()).isEqualTo(sortedExpected.get(i).getLength());
            }
            assertThat(sortedActual.get(i).getModificationTime()).isGreaterThan(0);
        }
    }

    private void prepareFilesOnStorage(HoodieStorage storage)
            throws IOException
    {
        String dir = getTempDir();
        for (String relativePath : RELATIVE_FILE_PATHS) {
            storage.create(new StoragePath(dir, relativePath)).close();
        }
    }
}
