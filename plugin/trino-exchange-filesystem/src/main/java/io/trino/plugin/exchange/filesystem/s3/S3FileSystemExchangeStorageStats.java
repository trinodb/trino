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
package io.trino.plugin.exchange.filesystem.s3;

import io.airlift.stats.DistributionStat;
import io.trino.plugin.exchange.filesystem.ExecutionStats;
import io.trino.plugin.exchange.filesystem.s3.S3AsyncClientWrapper.RequestType;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.exchange.filesystem.s3.S3AsyncClientWrapper.RequestType.ABORT_MULTIPART_UPLOAD;
import static io.trino.plugin.exchange.filesystem.s3.S3AsyncClientWrapper.RequestType.COMPLETE_MULTIPART_UPLOAD;
import static io.trino.plugin.exchange.filesystem.s3.S3AsyncClientWrapper.RequestType.CREATE_MULTIPART_UPLOAD;
import static io.trino.plugin.exchange.filesystem.s3.S3AsyncClientWrapper.RequestType.DELETE_OBJECTS;
import static io.trino.plugin.exchange.filesystem.s3.S3AsyncClientWrapper.RequestType.GET_OBJECT;
import static io.trino.plugin.exchange.filesystem.s3.S3AsyncClientWrapper.RequestType.LIST_OBJECTS_V2;
import static io.trino.plugin.exchange.filesystem.s3.S3AsyncClientWrapper.RequestType.PUT_OBJECT;
import static io.trino.plugin.exchange.filesystem.s3.S3AsyncClientWrapper.RequestType.UPLOAD_PART;

public class S3FileSystemExchangeStorageStats
{
    private final ExecutionStats createEmptyFile = new ExecutionStats();
    private final ExecutionStats listFilesRecursively = new ExecutionStats();
    private final ExecutionStats deleteRecursively = new ExecutionStats();
    private final ExecutionStats deleteObjects = new ExecutionStats();
    private final DistributionStat deleteObjectsEntriesCount = new DistributionStat();
    private final ExecutionStats getObject = new ExecutionStats();
    private final DistributionStat getObjectDataSizeInBytes = new DistributionStat();
    private final ExecutionStats putObject = new ExecutionStats();
    private final DistributionStat putObjectDataSizeInBytes = new DistributionStat();
    private final ExecutionStats createMultipartUpload = new ExecutionStats();
    private final ExecutionStats uploadPart = new ExecutionStats();
    private final DistributionStat uploadPartDataSizeInBytes = new DistributionStat();
    private final ExecutionStats completeMultipartUpload = new ExecutionStats();
    private final DistributionStat completeMultipartUploadPartsCount = new DistributionStat();
    private final ExecutionStats abortMultipartUpload = new ExecutionStats();
    private final Map<RequestType, AtomicLong> activeRequests = new ConcurrentHashMap<>();

    @Managed
    @Nested
    public ExecutionStats getCreateEmptyFile()
    {
        return createEmptyFile;
    }

    @Managed
    @Nested
    public ExecutionStats getListFilesRecursively()
    {
        return listFilesRecursively;
    }

    @Managed
    @Nested
    public ExecutionStats getDeleteRecursively()
    {
        return deleteRecursively;
    }

    @Managed
    @Nested
    public ExecutionStats getDeleteObjects()
    {
        return deleteObjects;
    }

    @Managed
    @Nested
    public DistributionStat getDeleteObjectsEntriesCount()
    {
        return deleteObjectsEntriesCount;
    }

    @Managed
    @Nested
    public ExecutionStats getGetObject()
    {
        return getObject;
    }

    @Managed
    @Nested
    public DistributionStat getGetObjectDataSizeInBytes()
    {
        return getObjectDataSizeInBytes;
    }

    @Managed
    @Nested
    public ExecutionStats getPutObject()
    {
        return putObject;
    }

    @Managed
    @Nested
    public DistributionStat getPutObjectDataSizeInBytes()
    {
        return putObjectDataSizeInBytes;
    }

    @Managed
    @Nested
    public ExecutionStats getCreateMultipartUpload()
    {
        return createMultipartUpload;
    }

    @Managed
    @Nested
    public ExecutionStats getUploadPart()
    {
        return uploadPart;
    }

    @Managed
    @Nested
    public DistributionStat getUploadPartDataSizeInBytes()
    {
        return uploadPartDataSizeInBytes;
    }

    @Managed
    @Nested
    public ExecutionStats getCompleteMultipartUpload()
    {
        return completeMultipartUpload;
    }

    @Managed
    @Nested
    public DistributionStat getCompleteMultipartUploadPartsCount()
    {
        return completeMultipartUploadPartsCount;
    }

    @Managed
    @Nested
    public ExecutionStats getAbortMultipartUpload()
    {
        return abortMultipartUpload;
    }

    public void requestStarted(RequestType requestType)
    {
        activeRequests.computeIfAbsent(requestType, key -> new AtomicLong()).incrementAndGet();
    }

    public void requestCompleted(RequestType requestType)
    {
        AtomicLong count = activeRequests.get(requestType);
        checkArgument(count != null && count.get() >= 0, "no active requests of type %s found", requestType);
        count.decrementAndGet();
    }

    @Managed
    public long getActivePutObjectRequestCount()
    {
        return getActiveRequestCount(PUT_OBJECT);
    }

    @Managed
    public long getActiveDeleteObjectsRequestCount()
    {
        return getActiveRequestCount(DELETE_OBJECTS);
    }

    @Managed
    public long getActiveGetObjectRequestCount()
    {
        return getActiveRequestCount(GET_OBJECT);
    }

    @Managed
    public long getActiveCreateMultipartUploadRequestCount()
    {
        return getActiveRequestCount(CREATE_MULTIPART_UPLOAD);
    }

    @Managed
    public long getActiveUploadPartRequestCount()
    {
        return getActiveRequestCount(UPLOAD_PART);
    }

    @Managed
    public long getActiveCompleteMultipartUploadRequestCount()
    {
        return getActiveRequestCount(COMPLETE_MULTIPART_UPLOAD);
    }

    @Managed
    public long getActiveAbortMultipartUploadRequestCount()
    {
        return getActiveRequestCount(ABORT_MULTIPART_UPLOAD);
    }

    @Managed
    public long getListObjectsV2RequestCount()
    {
        return getActiveRequestCount(LIST_OBJECTS_V2);
    }

    public Map<RequestType, Long> getActiveRequestsSummary()
    {
        return activeRequests.entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().get()));
    }

    private long getActiveRequestCount(RequestType requestType)
    {
        AtomicLong count = activeRequests.get(requestType);
        return count == null ? 0 : count.get();
    }
}
