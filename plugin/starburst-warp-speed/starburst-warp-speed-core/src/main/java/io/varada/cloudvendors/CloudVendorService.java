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

import com.google.common.annotations.VisibleForTesting;
import io.airlift.log.Logger;
import io.trino.filesystem.Location;
import io.varada.cloudvendors.model.StorageObjectMetadata;
import io.varada.tools.util.PathUtils;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.function.BiPredicate;

public abstract class CloudVendorService
{
    private static final Logger logger = Logger.get(CloudVendorService.class);

    private long requestRetryDelay = 500L;
    private long requestRetryMaxDelay = 8000L;
    private int requestRetryRetries = 5;

    @VisibleForTesting
    public void setRequestRetryDelay(long requestRetryDelay)
    {
        this.requestRetryDelay = requestRetryDelay;
    }

    @VisibleForTesting
    public void setRequestRetryMaxDelay(long requestRetryMaxDelay)
    {
        this.requestRetryMaxDelay = requestRetryMaxDelay;
    }

    @VisibleForTesting
    public void setRequestRetryRetries(int requestRetryRetries)
    {
        this.requestRetryRetries = requestRetryRetries;
    }

    public static String concatenatePath(String... parts)
    {
        if (parts == null || parts[0] == null) {
            return null;
        }
        if (parts.length == 1) {
            return parts[0];
        }
        return PathUtils.getUriPath(
                parts[0],
                Arrays.asList(parts).subList(1, parts.length).toArray(new String[] {}));
    }

    /**
     * List of string where bucket is first and the rest are the path parts
     * param str - S3 path
     */
    public Location getLocation(String path)
    {
        return Location.of(path);
    }

    public abstract void uploadToCloud(byte[] bytes, String path);

    public abstract void uploadFileToCloud(String localInputPath, String outputPath);

    public abstract boolean uploadFileToCloud(String path, File file, Callable<Boolean> validateBeforeDo);

    public abstract Optional<String> downloadCompressedFromCloud(
            String path,
            boolean allowKeyNotFound)
            throws IOException;

    public abstract InputStream downloadRangeFromCloud(String path, long startOffset, int length);

    public abstract void downloadFileFromCloud(String path, File file);

    public abstract boolean appendOnCloud(String path, File file, long startOffset, boolean isSparseFile, Callable<Boolean> validateBeforeDo);

    public List<String> listPath(String path)
    {
        return listPath(path, true);
    }

    public abstract List<String> listPath(String path, boolean isTopLevel);

    private void append(File file, File destFile, long startOffset)
    {
        try (RandomAccessFile inputRandomAccessFile = new RandomAccessFile(file, "rw");
                RandomAccessFile outputRandomAccessFile = new RandomAccessFile(destFile, "rw")) {
            byte[] buffer = new byte[8192 * 100]; // PageSize * 100
            int length;

            inputRandomAccessFile.seek(startOffset);
            outputRandomAccessFile.seek(startOffset);
            while ((length = inputRandomAccessFile.read(buffer)) > 0) {
                outputRandomAccessFile.write(buffer, 0, length);
            }
        }
        catch (IOException e) {
            logger.error("append failed. message: %s", e.getMessage());
            throw new RuntimeException("failed to append");
        }
    }

    public boolean appendOnLocal(String path, File file, long startOffset, Callable<Boolean> validateBeforeDo)
    {
        File destFile = new File(file.getPath() + getTempFileSuffix());

        logger.debug("appendOnLocal file [%s] length %d startOffset %d => path [%s] destFile [%s]",
                file, file.length(), startOffset, path, destFile);
        downloadFileFromCloud(path, destFile);
        append(file, destFile, startOffset);
        boolean isUploadDone = uploadFileToCloud(path, destFile, validateBeforeDo);
        FileUtils.deleteQuietly(destFile);
        return isUploadDone;
    }

    public abstract boolean directoryExists(String path);

    public abstract StorageObjectMetadata getObjectMetadata(String path);

    public abstract Optional<Long> getLastModified(String path);

    protected String getTempFileSuffix()
    {
        return LocalDateTime.now(ZoneId.systemDefault()).format(DateTimeFormatter.ofPattern("_yyyyMMdd_HHmmss_SSS"));
    }

    protected <T> T executeRequestUnderRetry(Callable<T> request, BiPredicate<T, ? extends Throwable> completionPredicate)
    {
        return Failsafe
                .with(new RetryPolicy<T>()
                        .withMaxRetries(requestRetryRetries)
                        .withBackoff(requestRetryDelay, requestRetryMaxDelay, ChronoUnit.MILLIS)
                        .abortIf(completionPredicate)
                        .onRetry(event -> logger.warn(event.getLastFailure(), "failed to execute cloud request, retrying. Attempt=%d", event.getAttemptCount())))
                .get(context -> request.call());
    }
}
