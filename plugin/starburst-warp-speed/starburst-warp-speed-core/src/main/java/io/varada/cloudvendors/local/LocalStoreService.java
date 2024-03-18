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

import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.airlift.log.Logger;
import io.varada.cloudvendors.CloudVendorService;
import io.varada.cloudvendors.model.StorageObjectMetadata;
import io.varada.tools.util.CompressionUtil;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;

import static java.util.Objects.requireNonNull;

@Singleton
public class LocalStoreService
        extends CloudVendorService
{
    private static final Logger logger = Logger.get(LocalStoreService.class);

    @Inject
    public LocalStoreService() {}

    @Override
    public void uploadToCloud(byte[] bytes, String path)
    {
        validateLocation(path);
        Path filePath = getPath(path);

        try {
            Path dir = Files.createDirectories(filePath.getParent());

            if (dir.toFile().exists()) {
                Path file = Files.write(filePath, bytes);
                if (!file.toFile().exists()) {
                    throw new RuntimeException("failed to create/write to file " + file);
                }
            }
            else {
                throw new RuntimeException("failed to create folder " + dir);
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        logger.debug("uploaded to file %s", path);
    }

    @Override
    public void uploadFileToCloud(String localInputPath, String outputPath)
    {
        validateLocation(localInputPath);
        validateLocation(outputPath);

        try {
            Path dstPath = getPath(outputPath);
            Files.createDirectories(dstPath.getParent().toAbsolutePath());
            Files.copy(getPath(localInputPath), dstPath);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean uploadFileToCloud(String path, File file, Callable<Boolean> validateBeforeDo)
    {
        validateLocation(path);

        try {
            Path filePath = getPath(path);
            Path dir = Files.createDirectories(filePath.getParent());
            if (dir.toFile().exists()) {
                if (filePath.toFile().exists()) {
                    Files.delete(filePath);
                }
                Files.copy(file.toPath(), filePath);
            }
            else {
                throw new RuntimeException("failed to create folder " + dir);
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        return true;
    }

    @Override
    public Optional<String> downloadCompressedFromCloud(
            String path,
            boolean allowKeyNotFound)
            throws IOException
    {
        validateLocation(path);

        Path filePath = getPath(path);
        return Optional.ofNullable(
                filePath.toFile().exists() ?
                        CompressionUtil.decompressGzip(Files.readAllBytes(filePath)) :
                        null);
    }

    @Override
    public InputStream downloadRangeFromCloud(String path, long startOffset, int length)
    {
        validateLocation(path);

        try {
            Path filePath = getPath(path);
            if (filePath.toFile().exists()) {
                byte[] bytes = Files.readAllBytes(filePath);
                return new InputStream()
                {
                    int index = (int) startOffset;

                    @Override
                    public int read()
                    {
                        if (index >= (startOffset + length)) {
                            return -1;
                        }
                        return bytes[index++];
                    }

                    @Override
                    public int read(byte[] b, int off, int len)
                    {
                        if (index >= (startOffset + length)) {
                            return -1;
                        }
                        int actualLen = (int) Math.min(len, startOffset + length - index);
                        System.arraycopy(bytes, index, b, off, actualLen);
                        index += actualLen;
                        return actualLen;
                    }
                };
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        return InputStream.nullInputStream();
    }

    @Override
    public void downloadFileFromCloud(String path, File file)
    {
        validateLocation(path);

        try {
            Path filePath = getPath(path);
            if (filePath.toFile().exists()) {
                Files.write(file.toPath(), Files.readAllBytes(filePath));
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean appendOnCloud(String path, File localFile, long startOffset, boolean isSparseFile, Callable<Boolean> validateBeforeDo)
    {
        validateLocation(path);

        if (isSparseFile) {
            throw new RuntimeException("sparse file is not supported for append on cloud");
        }

        Path cloudPath = getPath(path);
        if (!cloudPath.toFile().exists()) {
            throw new RuntimeException("file does not exist on cloud but append was requested path " + path);
        }

        byte[] bytes;
        try (RandomAccessFile localRandomAccessFile = new RandomAccessFile(localFile, "r")) {
            localRandomAccessFile.seek(startOffset);
            long bytesToCopy = localRandomAccessFile.length() - startOffset;
            bytes = new byte[(int) bytesToCopy];
            int readBytes = localRandomAccessFile.read(bytes);
            if (readBytes <= 0) {
                throw new RuntimeException("failed to read " + bytesToCopy + " bytes from path " + path);
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }

        try (RandomAccessFile cloudRandomAccessFile = new RandomAccessFile(cloudPath.toFile(), "rw")) {
            cloudRandomAccessFile.seek(startOffset);
            cloudRandomAccessFile.write(bytes);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        return true;
    }

    @Override
    public List<String> listPath(String path, boolean isTopLevel)
    {
        validateLocation(path);

        List<String> result = new ArrayList<>();

        File file = getPath(path).toFile();
        if (file.exists()) {
            if (file.isDirectory()) {
                List<String> list = Arrays.stream(requireNonNull(file.list()))
                        .map(s -> concatenatePath(path, s))
                        .toList();
                if (!isTopLevel) {
                    result.addAll(list.stream()
                            .flatMap(s -> listPath(s, false).stream())
                            .toList());
                }
                else {
                    result.addAll(list);
                }
            }
            else {
                result.add(file.getAbsolutePath());
            }
        }
        return result;
    }

    @Override
    public boolean directoryExists(String path)
    {
        validateLocation(path);

        Path filePath = getPath(path);
        return filePath.toFile().exists();
    }

    @Override
    public StorageObjectMetadata getObjectMetadata(String path)
    {
        validateLocation(path);

        Path filePath = getPath(path);
        StorageObjectMetadata storageObjectMetadata = new StorageObjectMetadata();
        if (filePath.toFile().exists()) {
            storageObjectMetadata.setContentLength(filePath.toFile().length());
            storageObjectMetadata.setLastModified(filePath.toFile().lastModified());
        }
        return storageObjectMetadata;
    }

    @Override
    public Optional<Long> getLastModified(String path)
    {
        validateLocation(path);

        StorageObjectMetadata storageObjectMetadata = getObjectMetadata(path);
        return storageObjectMetadata.getLastModified();
    }

    private Path getPath(String path, String... pathElements)
    {
        String cleanPath = path.endsWith("/") ? path.substring(0, path.length() - 1) : path;
        return Path.of(cleanPath.replaceFirst("file://", ""), pathElements);
    }

    private void validateLocation(String path)
    {
        requireNonNull(getLocation(path));
    }
}
