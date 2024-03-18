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
package io.trino.plugin.warp.extension.execution.callhome;

import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.Request;
import io.airlift.log.Logger;
import io.trino.plugin.varada.annotations.ForWarp;
import io.trino.spi.HostAddress;
import io.varada.cloudvendors.CloudVendorService;
import io.varada.tools.util.Pair;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toCollection;

public class CallHomeJob
        implements Runnable
{
    private static final Logger logger = Logger.get(CallHomeJob.class);

    private final CloudVendorService cloudVendorService;
    private final HostAddress nodeAddress;
    private final String storePath;
    private final String serverLogPath;
    private final String catalogPath;
    private final boolean collectThreadDumps;
    private int numberOfUploaded;

    public CallHomeJob(
            @ForWarp CloudVendorService cloudVendorService,
            HostAddress nodeAddress,
            String storePath,
            String serverLogPath,
            String catalogPath,
            boolean collectThreadDumps)
    {
        this.cloudVendorService = requireNonNull(cloudVendorService);
        this.nodeAddress = requireNonNull(nodeAddress);
        this.storePath = requireNonNull(storePath);
        this.serverLogPath = requireNonNull(serverLogPath);
        this.catalogPath = requireNonNull(catalogPath);
        this.collectThreadDumps = collectThreadDumps;
    }

    @Override
    public void run()
    {
        Set<Pair<Path, String>> filesToUpload = new HashSet<>();
        filesToUpload.addAll(collectLogs(storePath, serverLogPath));
        filesToUpload.addAll(collectConnectorProperties(storePath, catalogPath));
        filesToUpload.addAll(collectNodeProperties(storePath, catalogPath));
        numberOfUploaded = uploadFiles(filesToUpload, storePath);

        if (collectThreadDumps) {
            collectThreadDumps(storePath);
        }
    }

    public int getNumberOfUploaded()
    {
        return numberOfUploaded;
    }

    @SuppressWarnings("unused")
    private void collectThreadDumps(String uploadPath)
    {
        try {
            HttpUriBuilder threadDumpUriBuilder = uriBuilderFrom(URI.create("http://" + nodeAddress.getHostText() + ":" + nodeAddress.getPortOrDefault(8080)));
            threadDumpUriBuilder.appendPath("v1").appendPath("thread");

            Request request = prepareGet()
                    .setUri(threadDumpUriBuilder.build())
                    .setHeader("Content-Type", "application/json")
                    .setHeader("X-Trino-User", "trino")
                    .build();
        }
        catch (Exception e) {
            logger.warn(e, "failed uploading thread dump");
        }
    }

    private Collection<? extends Pair<Path, String>> collectNodeProperties(String targetPath, String sourcePath)
    {
        String nodePropertiesPath = new File(sourcePath).getParentFile().getAbsolutePath();
        return collectFiles(targetPath, nodePropertiesPath);
    }

    private Set<Pair<Path, String>> collectConnectorProperties(String targetPath, String sourcePath)
    {
        return collectFiles(targetPath, sourcePath);
    }

    private Set<Pair<Path, String>> collectFiles(String targetPath, String sourcePath)
    {
        Set<Pair<Path, String>> ret = new HashSet<>();
        try (Stream<Path> pathStream = Files.list(Path.of(sourcePath))) {
            pathStream.filter(propertiesFile -> !propertiesFile.toFile().isDirectory() &&
                            cloudVendorService.getLastModified(
                                    CloudVendorService.concatenatePath(targetPath, propertiesFile.getFileName().toString())).isEmpty())
                    .forEach(propertiesFile -> ret.add(Pair.of(propertiesFile, propertiesFile.getFileName().toString())));
        }
        catch (IOException e) {
            logger.warn(e, "failed to collect connectors path [%s]", sourcePath);
        }
        return ret;
    }

    private Set<Pair<Path, String>> collectLogs(String targetPath, String sourcePath)
    {
        String directory = new File(sourcePath).getParent();

        return Stream.of("server.log", "launcher.log", "gc.log")
                .flatMap(fileName -> {
                    try {
                        return addNonExistingFiles(targetPath, directory, fileName).stream();
                    }
                    catch (Exception e) {
                        logger.warn("failed to collect log %s, msg=%s", fileName, e.getMessage());
                    }
                    return Stream.of();
                }).collect(Collectors.toSet());
    }

    private Set<Pair<Path, String>> addNonExistingFiles(
            String storePath,
            String directory,
            String fileName)
    {
        try (Stream<Path> pathStream = Files.list(Path.of(directory))) {
            Set<Pair<Path, String>> ret = pathStream
                    .filter(localFilePath -> localFilePath.getFileName().toString().contains(fileName))
                    .filter(localFilePath -> shouldUploadFile(storePath, localFilePath))
                    .map(localFile -> Pair.of(localFile, localFile.getFileName().toString()))
                    .collect(toCollection(HashSet::new));
            ret.add(Pair.of(
                    Path.of(new URI(CloudVendorService.concatenatePath("file:/", directory, fileName))),
                    fileName));
            return ret;
        }
        catch (Exception e) {
            logger.warn(e, "failed to collect connectors files, msg=%s", e.getMessage());
        }
        return Set.of();
    }

    private boolean shouldUploadFile(String storePath, Path fileToCopy)
    {
        return cloudVendorService.listPath(
                CloudVendorService.concatenatePath(
                        storePath,
                        fileToCopy.getFileName().toString())).isEmpty();
    }

    protected int uploadFiles(Set<Pair<Path, String>> filesToUpload, String uploadPath)
    {
        logger.debug("uploadFiles -> uploadPath=%s -- filesToUpload=%s", uploadPath, filesToUpload);
        // we'll try to copy the files into a temporary directory because the original files might be changed while uploading
        // and this may cause the upload to fail with "AmazonS3Exception: The Content-MD5 you specified did not match what we received."
        Optional<Path> tempDirectory;
        try {
            tempDirectory = Optional.of(Files.createTempDirectory("call_home"));
        }
        catch (IOException e) {
            tempDirectory = Optional.empty();
        }

        int numberOfUploaded = 0;
        StringBuilder traceLogBuilder = new StringBuilder();
        try {
            for (Pair<Path, String> fileToUpload : filesToUpload) {
                Optional<File> tempFile = Optional.empty();
                try {
                    File srcFile = fileToUpload.getLeft().toFile();
                    tempFile = tempDirectory.flatMap(directory -> tryToCopyFile(srcFile, directory));
                    File file = tempFile.orElse(srcFile);
                    String key = String.join("/", uploadPath, fileToUpload.getRight());
                    cloudVendorService.uploadFileToCloud(file.getPath(), key);
                    traceLogBuilder.append(String.format("uploading file %s to %s", file.getPath(), key));
                    numberOfUploaded++;
                    logger.debug("uploaded file %s to %s", file.getPath(), key);
                }
                catch (Exception e) {
                    logger.warn(e, "failed to upload file %s", fileToUpload);
                }
                finally {
                    tempFile.ifPresent(FileUtils::deleteQuietly);
                }
            }
        }
        finally {
            tempDirectory.ifPresent(path -> FileUtils.deleteQuietly(path.toFile()));
            logger.debug(traceLogBuilder.toString());
        }
        return numberOfUploaded;
    }

    public Optional<File> tryToCopyFile(File srcFile, Path targetDirectory)
    {
        if (srcFile.exists()) {
            try {
                File tmpFile = new File(targetDirectory.toString(), srcFile.getName());
                FileUtils.copyFile(srcFile, tmpFile);
                return Optional.of(tmpFile);
            }
            catch (Exception e) {
                logger.error(e, "failed to copy file %s into directory %s", srcFile, targetDirectory);
            }
        }
        return Optional.empty();
    }
}
