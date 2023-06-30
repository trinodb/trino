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
package io.trino.plugin.deltalake.transactionlog.writer;

import com.google.cloud.hadoop.repackaged.gcs.com.google.api.client.http.ByteArrayContent;
import com.google.cloud.hadoop.repackaged.gcs.com.google.api.services.storage.Storage;
import com.google.cloud.hadoop.repackaged.gcs.com.google.api.services.storage.model.StorageObject;
import com.google.cloud.hadoop.repackaged.gcs.com.google.cloud.hadoop.gcsio.StorageResourceId;
import com.google.inject.Inject;
import io.trino.filesystem.Location;
import io.trino.plugin.deltalake.GcsStorageFactory;
import io.trino.spi.connector.ConnectorSession;

import java.io.IOException;
import java.io.UncheckedIOException;

import static java.util.Objects.requireNonNull;

public class GcsTransactionLogSynchronizer
        implements TransactionLogSynchronizer
{
    private final GcsStorageFactory gcsStorageFactory;

    @Inject
    public GcsTransactionLogSynchronizer(GcsStorageFactory gcsStorageFactory)
    {
        this.gcsStorageFactory = requireNonNull(gcsStorageFactory, "gcsStorageFactory is null");
    }

    // This approach is compatible with OSS Delta Lake
    // https://github.com/delta-io/delta/blob/225e2bbf9ecaf034d08ef8d2fee1929e51c951bf/storage/src/main/java/io/delta/storage/GCSLogStore.java
    // This approach differs from the OSS Delta Lake corresponding implementation
    // in the sense that it uses the `Storage` GCS API client directly (instead of Hadoop's `FSDataOutputStream`)
    // in order to avoid leaked output streams in case of I/O exceptions occurring while uploading
    // the blob content.
    @Override
    public void write(ConnectorSession session, String clusterId, Location newLogEntryPath, byte[] entryContents)
    {
        Storage storage = gcsStorageFactory.create(session, newLogEntryPath.toString());
        try {
            createStorageObjectExclusively(newLogEntryPath.toString(), entryContents, storage);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public boolean isUnsafe()
    {
        return false;
    }

    /**
     * This method uses {@link Storage} Cloud Storage API client for creating synchronously a storage object
     * with the specified path and content.
     * <p>
     * This approach avoids dealing with `FSDataOutputStream` exposed by the method
     * `org.apache.hadoop.fs.FileSystem#create(org.apache.hadoop.fs.Path)` and the intricacies of handling
     * exceptions which may occur while writing the content to the output stream.
     */
    private static void createStorageObjectExclusively(String blobPath, byte[] content, Storage storage)
            throws IOException
    {
        StorageResourceId storageResourceId = StorageResourceId.fromStringPath(blobPath);
        Storage.Objects.Insert insert = storage.objects().insert(
                storageResourceId.getBucketName(),
                new StorageObject()
                        .setName(storageResourceId.getObjectName()),
                new ByteArrayContent("application/json", content));
        // By setting `ifGenerationMatch` setting to `0`, the creation of the blob will succeed only
        // if there are no live versions of the object. When the blob already exists, the operation
        // will fail with the exception message `412 Precondition Failed`
        insert.setIfGenerationMatch(0L);
        insert.getMediaHttpUploader().setDirectUploadEnabled(true);
        insert.execute();
    }
}
