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

import com.google.common.collect.ImmutableList;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.local.LocalFileSystemFactory;
import io.trino.testing.connector.TestingConnectorSession;
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
import java.util.Comparator;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.apache.hudi.storage.HoodieInstantWriter.convertByteArrayToWriter;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestTrinoHudiStorage
        extends TestHoodieStorageBase
{
    private static final byte[] EMPTY_BYTES = new byte[] {};

    private static TrinoFileSystem fileSystem;

    @BeforeAll
    static void setUp()
    {
        fileSystem = new LocalFileSystemFactory(Path.of("/tmp"))
                .create(TestingConnectorSession.SESSION);
    }

    @Override
    protected HoodieStorage getStorage(Object fileSystem, Object config)
    {
        return new TrinoHudiStorage((TrinoFileSystem) fileSystem, (TrinoStorageConfiguration) config);
    }

    @Override
    protected TrinoFileSystem getFileSystem(Object config)
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
        // As the LocalFileSystemFactory is used, the path should have "local:" as the scheme
        return "local:" + this.tempDir.toUri().getPath();
    }

    @Override
    @Test
    public void testGetUri()
            throws URISyntaxException
    {
        assertThat(getStorage().getUri()).isEqualTo(new URI(""));
    }

    // This test is overridden since TrinoHudiStorage does not support globEntries,
    // as it is not used in the Trino Hudi connector
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
                storage.listDirectEntries(new StoragePath(getTempDir(), "x")),
                ImmutableList.<StoragePathInfo>builder()
                        .add(getStoragePathInfo("x/1.file", false))
                        .add(getStoragePathInfo("x/2.file", false))
                        .add(getStoragePathInfo("x/y", true))
                        .add(getStoragePathInfo("x/z", true))
                        .build());

        validatePathInfoList(
                storage.listFiles(new StoragePath(getTempDir(), "x")),
                ImmutableList.<StoragePathInfo>builder()
                        .add(getStoragePathInfo("x/1.file", false))
                        .add(getStoragePathInfo("x/2.file", false))
                        .add(getStoragePathInfo("x/y/1.file", false))
                        .add(getStoragePathInfo("x/y/2.file", false))
                        .add(getStoragePathInfo("x/z/1.file", false))
                        .add(getStoragePathInfo("x/z/2.file", false))
                        .build());

        validatePathInfoList(
                storage.listDirectEntries(
                        new StoragePath(getTempDir(), "x"), e -> e.getName().contains("2")),
                ImmutableList.<StoragePathInfo>builder()
                        .add(getStoragePathInfo("x/2.file", false))
                        .build());

        validatePathInfoList(
                storage.listDirectEntries(ImmutableList.<StoragePath>builder()
                                .add(new StoragePath(getTempDir(), "w"))
                                .add(new StoragePath(getTempDir(), "x/z"))
                                .build()),
                ImmutableList.<StoragePathInfo>builder()
                        .add(getStoragePathInfo("w/1.file", false))
                        .add(getStoragePathInfo("w/2.file", false))
                        .add(getStoragePathInfo("x/z/1.file", false))
                        .add(getStoragePathInfo("x/z/2.file", false))
                        .build());

        assertThatThrownBy(
                () -> storage.listDirectEntries(new StoragePath(getTempDir(), "*")))
                .isInstanceOf(FileNotFoundException.class);

        // TrinoHudiStorage does not support globEntries, as it is not used
        // in the Trino Hudi connector
        assertThatThrownBy(
                () -> storage.globEntries(new StoragePath(getTempDir(), "x/*/1.file")))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    // This test is overridden since TrinoHudiStorage always returns true for deletion,
    // because TrinoFileSystem does not indicate whether the file to delete exists or not
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

    // This test is overridden because TrinoFileSystem does not overwrite the file with
    // #create so the test logic has to be adapted (not that this behavior does not affect
    // product code logic in Trino Hudi connector as it's read-only)
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
        storage.createImmutableFileInPath(path3, Option.of(convertByteArrayToWriter(data)));
        validatePathInfo(storage, path3, data, false);

        StoragePath path4 = new StoragePath(getTempDir(), "testCreateAppendAndRead/4");
        assertThat(storage.exists(path4)).isFalse();
        assertThat(storage.createDirectory(path4)).isTrue();
        validatePathInfo(storage, path4, EMPTY_BYTES, true);
        assertThat(storage.createDirectory(path4)).isTrue();
    }

    private HoodieStorage getStorage()
    {
        Object config = getConf();
        return getStorage(getFileSystem(config), config);
    }

    private StoragePathInfo getStoragePathInfo(String subPath, boolean isDirectory)
    {
        return new StoragePathInfo(new StoragePath(getTempDir(), subPath),
                0, isDirectory, (short) 1, 1000000L, 10L);
    }

    private static void validatePathInfo(
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

    private static void validatePathInfoList(
            List<StoragePathInfo> actual,
            List<StoragePathInfo> expected)
    {
        assertThat(actual).hasSize(expected.size());
        List<StoragePathInfo> sortedExpected = expected.stream()
                .sorted(Comparator.comparing(StoragePathInfo::getPath))
                .collect(toImmutableList());
        List<StoragePathInfo> sortedActual = actual.stream()
                .sorted(Comparator.comparing(StoragePathInfo::getPath))
                .collect(toImmutableList());
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
        String directory = getTempDir();
        for (String relativePath : RELATIVE_FILE_PATHS) {
            storage.create(new StoragePath(directory, relativePath)).close();
        }
    }
}
