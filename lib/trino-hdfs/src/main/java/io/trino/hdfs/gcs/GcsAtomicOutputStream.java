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
package io.trino.hdfs.gcs;

import com.google.cloud.hadoop.repackaged.gcs.com.google.api.client.http.ByteArrayContent;
import com.google.cloud.hadoop.repackaged.gcs.com.google.api.services.storage.Storage;
import com.google.cloud.hadoop.repackaged.gcs.com.google.api.services.storage.model.StorageObject;
import com.google.cloud.hadoop.repackaged.gcs.com.google.cloud.hadoop.gcsio.StorageResourceId;
import io.trino.hdfs.HdfsContext;
import io.trino.hdfs.HdfsEnvironment;
import org.apache.hadoop.fs.Path;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class GcsAtomicOutputStream
        extends ByteArrayOutputStream
{
    private final Storage storage;
    private final Path path;
    private boolean closed;

    public GcsAtomicOutputStream(HdfsEnvironment environment, HdfsContext context, Path path)
    {
        this.storage = environment.createGcsStorage(context, path);
        this.path = path;
    }

    @Override
    public void close()
            throws IOException
    {
        if (closed) {
            return;
        }
        closed = true;

        StorageResourceId storageResourceId = StorageResourceId.fromStringPath(path.toString());
        Storage.Objects.Insert insert = storage.objects().insert(
                storageResourceId.getBucketName(),
                new StorageObject().setName(storageResourceId.getObjectName()),
                new ByteArrayContent("application/octet-stream", buf, 0, count));
        insert.setIfGenerationMatch(0L); // fail if object already exists
        insert.getMediaHttpUploader().setDirectUploadEnabled(true);
        insert.execute();
    }
}
