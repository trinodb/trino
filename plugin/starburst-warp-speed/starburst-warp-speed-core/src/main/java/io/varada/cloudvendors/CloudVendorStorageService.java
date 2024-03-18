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

import io.airlift.log.Logger;
import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoOutputFile;
import io.varada.cloudstorage.CloudStorage;
import io.varada.cloudvendors.model.StorageObjectMetadata;
import io.varada.tools.ByteBufferInputStream;
import io.varada.tools.util.CompressionUtil;
import io.varada.tools.util.StringUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;

import static java.util.Objects.requireNonNull;

public class CloudVendorStorageService
        extends CloudVendorService
{
    private static final Logger logger = Logger.get(CloudVendorStorageService.class);

    private final CloudStorage cloudStorage;

    public CloudVendorStorageService(CloudStorage cloudStorage)
    {
        this.cloudStorage = requireNonNull(cloudStorage);
    }

    @Override
    public void uploadToCloud(byte[] bytes, String outputPath)
    {
        Location location = getLocation(outputPath);
        TrinoOutputFile outputFile = cloudStorage.newOutputFile(location);

        String message = String.format("uploadToCloud bytes length %d => location [%s]", bytes.length, location);
        logger.debug(message);

        try (OutputStream outputStream = outputFile.create()) {
            outputStream.write(bytes);
        }
        catch (IOException e) {
            throw new RuntimeException(message, e);
        }
    }

    @Override
    public void uploadFileToCloud(String localInputPath, String outputPath)
    {
        if (StringUtils.isEmpty(localInputPath) || StringUtils.isEmpty(outputPath)) {
            throw new IllegalArgumentException("in/out path is null/empty");
        }
        if (!new File(localInputPath).exists()) {
            throw new IllegalArgumentException("input path file doesnt exist");
        }
        try {
            logger.debug("uploadFileToCloud [%s] => [%s]", getLocation(localInputPath), getLocation(outputPath));
            cloudStorage.uploadFile(getLocation(localInputPath), getLocation(outputPath));
        }
        catch (Exception e) {
            throw new RuntimeException("uploadFileToCloud failed [%s] => [%s]".formatted(localInputPath, outputPath), e);
        }
    }

    @Override
    public boolean uploadFileToCloud(String outputPath, File localFile, Callable<Boolean> validateBeforeDo)
    {
        Location destination = getLocation(outputPath + getTempFileSuffix());
        boolean isUploadDone = false;

        try {
            logger.debug("uploadFileToCloud [%s] => [%s]", getLocation(localFile.getPath()), destination);
            cloudStorage.uploadFile(getLocation(localFile.getPath()), destination);

            if ((validateBeforeDo == null) || validateBeforeDo.call()) {
                cloudStorage.renameFile(destination, getLocation(outputPath));
                isUploadDone = true;
            }
            else {
                cloudStorage.deleteFile(destination);
            }
        }
        catch (Exception e) {
            logger.error(e, "uploadFileToCloud failed");
            throw new RuntimeException("uploadFileToCloud failed [%s] => [%s]".formatted(localFile.getPath(), outputPath), e);
        }
        return isUploadDone;
    }

    @Override
    public Optional<String> downloadCompressedFromCloud(String cloudPath, boolean allowKeyNotFound)
    {
        Location location = getLocation(cloudPath);
        TrinoInputFile inputFile = cloudStorage.newInputFile(location);

        String message = String.format("downloadCompressedFromCloud location [%s]", location);

        try (TrinoInput input = inputFile.newInput()) {
            int length = (int) inputFile.length();
            byte[] bytes = new byte[length];

            logger.debug("%s => bytes length %d", message, bytes.length);

            input.readFully(0, bytes, 0, length);
            String json = CompressionUtil.decompressGzip(bytes);

            return Optional.of(json);
        }
        catch (IOException e) {
            if (allowKeyNotFound) {
                return Optional.empty();
            }
            throw new RuntimeException(message, e);
        }
    }

    @Override
    public InputStream downloadRangeFromCloud(String cloudPath, long startOffset, int length)
    {
        Location location = getLocation(cloudPath);
        TrinoInputFile inputFile = cloudStorage.newInputFile(location);

        String message = String.format("downloadRangeFromCloud location [%s] startOffset %d length %d",
                location, startOffset, length);
        logger.debug(message);

        try (TrinoInput input = inputFile.newInput()) {
            byte[] bytes = new byte[length];

            input.readFully(startOffset, bytes, 0, length);
            return new ByteBufferInputStream(ByteBuffer.wrap(bytes), length);
        }
        catch (IOException e) {
            throw new RuntimeException(message, e);
        }
    }

    @Override
    public void downloadFileFromCloud(String cloudPath, File localFile)
    {
        try {
            logger.debug("downloadFileFromCloud [%s] => [%s]", getLocation(cloudPath), getLocation(localFile.getPath()));
            cloudStorage.downloadFile(getLocation(cloudPath), getLocation(localFile.getPath()));
        }
        catch (Exception e) {
            throw new RuntimeException("downloadFileFromCloud failed [%s] => [%s]".formatted(cloudPath, localFile.getPath()), e);
        }
    }

    @Override
    public boolean appendOnCloud(String cloudPath, File localFile, long startOffset, boolean isSparseFile, Callable<Boolean> validateBeforeDo)
    {
        Location source = getLocation(cloudPath);
        Location destination = getLocation(cloudPath + getTempFileSuffix());

        String message = String.format("appendOnCloud source [%s] => destination [%s] startOffset %d", source, destination, startOffset);
        logger.debug(message);

        try (RandomAccessFile randomAccessFile = new RandomAccessFile(localFile, "rw")) {
            int length = (int) (localFile.length() - startOffset);
            byte[] buffer = new byte[length];

            randomAccessFile.seek(startOffset);
            length = randomAccessFile.read(buffer);

            if (length > 0) {
                cloudStorage.copyFileReplaceTail(source, destination, startOffset, buffer);
            }
        }
        catch (IOException e) {
            throw new RuntimeException(message, e);
        }

        boolean isUploadDone = false;

        try {
            if ((validateBeforeDo == null) || validateBeforeDo.call()) {
                cloudStorage.renameFile(destination, source);
                isUploadDone = true;
            }
            else {
                cloudStorage.deleteFile(destination);
            }
        }
        catch (Exception e) {
            throw new RuntimeException("appendOnCloud failed after copyFileReplaceTail", e);
        }
        return isUploadDone;
    }

    @Override
    public List<String> listPath(String cloudPath, boolean isTopLevel)
    {
        Location location = getLocation(cloudPath);

        String message = String.format("listPath location [%s] isTopLevel %s", location, isTopLevel);
        logger.debug(message);

        try {
            if (isTopLevel) {
                Set<Location> directories = cloudStorage.listDirectories(location);
                return directories.stream().map(Location::toString).toList();
            }
            else {
                FileIterator fileIterator = cloudStorage.listFiles(location);
                List<String> paths = new ArrayList<>();

                while (fileIterator.hasNext()) {
                    FileEntry fileEntry = fileIterator.next();
                    paths.add(fileEntry.location().toString());
                }
                return paths;
            }
        }
        catch (IOException e) {
            throw new RuntimeException(message, e);
        }
    }

    @Override
    public boolean directoryExists(String cloudPath)
    {
        // normalize to directory
        Location location = cloudPath.endsWith("/") ? getLocation(cloudPath) : getLocation(cloudPath + "/");
        try {
            logger.debug("directoryExists location [%s]", location);
            return cloudStorage.directoryExists(location).orElse(false);
        }
        catch (IOException e) {
            throw new RuntimeException("directoryExists [%s] failed".formatted(location), e);
        }
    }

    @Override
    public StorageObjectMetadata getObjectMetadata(String cloudPath)
    {
        StorageObjectMetadata storageObjectMetadata = new StorageObjectMetadata();
        Location location = getLocation(cloudPath);
        TrinoInputFile inputFile = cloudStorage.newInputFile(location);

        try {
            long length = inputFile.length();
            Instant lastModified = inputFile.lastModified();

            storageObjectMetadata.setContentLength(length);
            storageObjectMetadata.setLastModified(lastModified.toEpochMilli());
            logger.debug("getObjectMetadata location [%s] length %d lastModified %s", location, length, lastModified);
        }
        catch (IOException e) {
            // do nothing
//            throw new RuntimeException(e);
        }
        return storageObjectMetadata;
    }

    @Override
    public Optional<Long> getLastModified(String cloudPath)
    {
        Location location = getLocation(cloudPath);
        TrinoInputFile inputFile = cloudStorage.newInputFile(location);

        try {
            Instant lastModified = inputFile.lastModified();
            logger.debug("getLastModified location [%s] lastModified %s", location, lastModified);
            return Optional.of(lastModified.toEpochMilli());
        }
        catch (IOException e) {
            // do nothing
//            throw new RuntimeException(e);
        }
        return Optional.empty();
    }
}
