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
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

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
}
