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
package io.varada.cloudvendors.local;

import io.airlift.json.ObjectMapperProvider;
import io.varada.cloudvendors.model.StorageObjectMetadata;
import io.varada.tools.util.CompressionUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

public class LocalStoreServiceTest
{
    private LocalStoreService localStoreService;

    @BeforeEach
    public void beforeEach()
    {
        localStoreService = new LocalStoreService();
    }

    @Test
    public void testUploadToCloud()
            throws IOException
    {
        byte[] bytes = "bytes".getBytes(StandardCharsets.UTF_8);
        String path = Files.createTempDirectory("local_storage").toFile().getAbsolutePath();
        Path filePath = Path.of(path, "objectName");
        localStoreService.uploadToCloud(bytes, filePath.toString());
        assertThat(Files.readAllBytes(filePath)).isEqualTo(bytes);
    }

    @Test
    public void testUploadFileToCloud()
            throws IOException
    {
        File srcFile = Files.createTempFile("local_storage", "").toFile();
        byte[] bytes = "bytes".getBytes(StandardCharsets.UTF_8);
        Files.write(Path.of(srcFile.getAbsolutePath()), bytes);

        File dstFile = Files.createTempFile("local_storage", "").toFile();
        assertThat(Files.deleteIfExists(dstFile.toPath())).isTrue();

        localStoreService.uploadFileToCloud(srcFile.getAbsolutePath(), dstFile.getAbsolutePath());

        assertThat(Files.readAllBytes(Path.of(dstFile.getAbsolutePath()))).isEqualTo(bytes);

        assertThat(Files.deleteIfExists(dstFile.toPath())).isTrue();

        localStoreService.uploadFileToCloud(dstFile.getAbsolutePath(), srcFile, () -> true);

        assertThat(Files.readAllBytes(Path.of(dstFile.getAbsolutePath()))).isEqualTo(bytes);
    }

    @Test
    public void testDownloadCompressedFromCloud()
            throws IOException
    {
        File srcFile = Files.createTempFile("local_storage", "").toFile();
        String input = new ObjectMapperProvider().get().writeValueAsString(Map.of("k1", "v1"));
        byte[] bytes = CompressionUtil.compressGzip(input);
        Files.write(srcFile.toPath(), bytes);

        Optional<String> output = localStoreService.downloadCompressedFromCloud(
                srcFile.getAbsolutePath(),
                true);
        assertThat(output).isEqualTo(Optional.of(input));
    }

    @Test
    public void testDownloadRangeFromCloud()
            throws IOException
    {
        File srcFile = Files.createTempFile("local_storage", "").toFile();
        byte[] bytes = "bytes".getBytes(StandardCharsets.UTF_8);
        Files.write(Path.of(srcFile.getAbsolutePath()), bytes);

        try (InputStream inputStream = localStoreService.downloadRangeFromCloud(
                srcFile.getAbsolutePath(),
                2L,
                2)) {
            assertThat(inputStream.readAllBytes())
                    .isEqualTo(Arrays.copyOfRange(bytes, 2, 4));
        }
    }

    @Test
    public void testDownloadFileFromCloud()
            throws IOException
    {
        File srcFile = Files.createTempFile("local_storage", "").toFile();
        byte[] bytes = "bytes".getBytes(StandardCharsets.UTF_8);
        Files.write(Path.of(srcFile.getAbsolutePath()), bytes);

        File dstFile = Files.createTempFile("local_storage", "").toFile();
        assertThat(Files.deleteIfExists(dstFile.toPath())).isTrue();

        localStoreService.downloadFileFromCloud(
                srcFile.getAbsolutePath(),
                dstFile);

        assertThat(dstFile).exists();
        assertThat(Files.readAllBytes(dstFile.toPath())).isEqualTo(bytes);
    }

    @Test
    public void testAppendOnCloud()
            throws IOException
    {
        File srcFile = Files.createTempFile("local_storage", "").toFile();
        byte[] bytesResult = "12tes".getBytes(StandardCharsets.UTF_8);

        Files.writeString(Path.of(srcFile.getAbsolutePath()), "bytes");

        File dstFile = Files.createTempFile("local_storage", "").toFile();
        Files.writeString(Path.of(dstFile.getAbsolutePath()), "12abc");
        localStoreService.appendOnCloud(
                dstFile.getAbsolutePath(),
                srcFile,
                2,
                false,
                () -> true);

        assertThat(Files.readAllBytes(dstFile.toPath())).isEqualTo(bytesResult);
    }

    @Test
    public void testGetObjectMetadata()
            throws IOException
    {
        File srcFile = Files.createTempFile("local_storage", "").toFile();

        StorageObjectMetadata storageObjectMetadata = new StorageObjectMetadata();

        assertThat(localStoreService.getObjectMetadata(srcFile.getAbsolutePath() + "1"))
                .isEqualTo(storageObjectMetadata);

        storageObjectMetadata.setContentLength(srcFile.length());
        storageObjectMetadata.setLastModified(srcFile.lastModified());
        assertThat(localStoreService.getObjectMetadata(srcFile.getAbsolutePath()))
                .isEqualTo(storageObjectMetadata);
    }

    @Test
    public void testGetLastModified()
            throws IOException
    {
        File srcFile = Files.createTempFile("local_storage", "").toFile();

        StorageObjectMetadata storageObjectMetadata = new StorageObjectMetadata();

        assertThat(localStoreService.getLastModified(srcFile.getAbsolutePath() + "1"))
                .isEqualTo(storageObjectMetadata.getLastModified());

        storageObjectMetadata.setLastModified(srcFile.lastModified());
        assertThat(localStoreService.getLastModified(srcFile.getAbsolutePath()))
                .isEqualTo(storageObjectMetadata.getLastModified());
    }

    @Test
    public void testListPath()
            throws IOException
    {
        File srcFolder = Files.createTempDirectory("parent-folder").toFile();
        IntStream.range(1, 3).forEach(i -> {
            try {
                Files.createTempFile(Path.of(srcFolder.getPath()), "child-file-" + i, "");
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        IntStream.range(1, 5).forEach(i -> {
            try {
                File folder = Files.createTempDirectory(Path.of(srcFolder.getPath()), "folder-" + i).toFile();
                IntStream.range(1, 3).forEach(j -> {
                    try {
                        Files.createTempFile(Path.of(folder.getPath()), "file-" + j, "");
                    }
                    catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        assertThat(localStoreService.listPath(srcFolder.getPath())).hasSize(6);
        assertThat(localStoreService.listPath(srcFolder.getPath(), false)).hasSize(10);
    }
}
