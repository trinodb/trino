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
package io.varada.cloudvendors;

import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.varada.cloudstorage.CloudStorage;
import io.varada.tools.util.CompressionUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;

public class CloudVendorStorageServiceTest
{
    private CloudStorage cloudStorage;
    private CloudVendorStorageService cloudVendorStorageService;

    @BeforeEach
    void setUp()
    {
        cloudStorage = Mockito.mock(CloudStorage.class);
        cloudVendorStorageService = new CloudVendorStorageService(cloudStorage);
    }

    @Test
    public void test_downloadCompressedFromCloud()
            throws IOException
    {
        TrinoInputFile trinoInputFile = Mockito.mock(TrinoInputFile.class);
        Mockito.when(cloudStorage.newInputFile(any(Location.class))).thenReturn(trinoInputFile);

        TrinoInput trinoInput = Mockito.mock(TrinoInput.class);
        Mockito.when(trinoInputFile.newInput()).thenReturn(trinoInput);

        String expected = "downloadCompressedFromCloud";
        byte[] compressed = CompressionUtil.compressGzip(expected);
        Mockito.when(trinoInputFile.length()).thenReturn((long) compressed.length);

        Mockito.doAnswer(invocationOnMock -> {
            byte[] bytes = (byte[]) invocationOnMock.getArguments()[1];
            System.arraycopy(compressed, 0, bytes, 0, compressed.length);
            return null;
        }).when(trinoInput).readFully(anyLong(), any(), anyInt(), anyInt());

        Optional<String> result = cloudVendorStorageService.downloadCompressedFromCloud("s3://bucket/key", false);

        Assertions.assertEquals(expected, result.orElseThrow());
    }

    @Test
    void test_downloadRangeFromCloud()
            throws IOException
    {
        TrinoInputFile trinoInputFile = Mockito.mock(TrinoInputFile.class);
        Mockito.when(cloudStorage.newInputFile(any(Location.class))).thenReturn(trinoInputFile);

        TrinoInput trinoInput = Mockito.mock(TrinoInput.class);
        Mockito.when(trinoInputFile.newInput()).thenReturn(trinoInput);

        String expected = "downloadRangeFromCloud";
        byte[] asBytes = expected.getBytes(StandardCharsets.UTF_8);

        Mockito.doAnswer(invocationOnMock -> {
            byte[] bytes = (byte[]) invocationOnMock.getArguments()[1];
            System.arraycopy(asBytes, 0, bytes, 0, asBytes.length);
            return null;
        }).when(trinoInput).readFully(anyLong(), any(), anyInt(), anyInt());

        try (InputStream inputStream = cloudVendorStorageService.downloadRangeFromCloud("s3://bucket/key", 100, asBytes.length)) {
            Assertions.assertEquals(asBytes.length, inputStream.available());
            Assertions.assertArrayEquals(asBytes, inputStream.readAllBytes());
        }
    }

    @Test
    void test_listPath_dir()
            throws IOException
    {
        Set<Location> directories = new HashSet<>();
        Mockito.when(cloudStorage.listDirectories(any(Location.class))).thenReturn(directories);

        List<String> paths = cloudVendorStorageService.listPath("s3://bucket/dir", true);

        Assertions.assertTrue(paths.isEmpty());

        directories.add(Location.of("s3://bucket/dir/path1"));
        directories.add(Location.of("s3://bucket/dir/path2"));
        directories.add(Location.of("s3://bucket/dir/path3"));
        directories.add(Location.of("s3://bucket/dir/path4"));
        directories.add(Location.of("s3://bucket/dir/path5"));

        paths = cloudVendorStorageService.listPath("s3://bucket/dir", true);

        Assertions.assertEquals(directories.size(), paths.size());
        List<String> finalPaths = paths;
        directories.forEach(location -> Assertions.assertTrue(finalPaths.contains(location.toString())));
    }

    @Test
    void test_listPath_file()
            throws IOException
    {
        Mockito.when(cloudStorage.listFiles(any(Location.class))).thenReturn(new TestingFileIterator(List.of()));

        List<String> paths = cloudVendorStorageService.listPath("s3://bucket/dir", false);

        Assertions.assertTrue(paths.isEmpty());

        List<String> expected = List.of("s3://bucket/dir/path1", "s3://bucket/dir/path2", "s3://bucket/dir/path3");
        Mockito.when(cloudStorage.listFiles(any(Location.class))).thenReturn(new TestingFileIterator(expected));

        paths = cloudVendorStorageService.listPath("s3://bucket/dir", false);

        Assertions.assertEquals(expected, paths);
    }

    @Test
    void test_directoryExists()
            throws IOException
    {
        Mockito.when(cloudStorage.directoryExists(any(Location.class))).thenReturn(Optional.of(false)).thenThrow(new IOException("test"));

        boolean isExist = cloudVendorStorageService.directoryExists("s3://bucketName");
        Assertions.assertFalse(isExist);

        try {
            cloudVendorStorageService.directoryExists("s3://bucketName");
            Assertions.fail();
        }
        catch (RuntimeException e) {
            Assertions.assertEquals("directoryExists [s3://bucketName/] failed", e.getMessage());
        }
    }

    static class TestingFileIterator
            implements FileIterator
    {
        private final Iterator<FileEntry> iterator;

        TestingFileIterator(List<String> paths)
        {
            iterator = paths.stream()
                    .map(path -> new FileEntry(
                            Location.of(path),
                            1L,
                            Instant.now(),
                            Optional.empty()))
                    .toList()
                    .iterator();
        }

        @Override
        public boolean hasNext()
        {
            return iterator.hasNext();
        }

        @Override
        public FileEntry next()
                throws IOException
        {
            return iterator.next();
        }
    }
}
