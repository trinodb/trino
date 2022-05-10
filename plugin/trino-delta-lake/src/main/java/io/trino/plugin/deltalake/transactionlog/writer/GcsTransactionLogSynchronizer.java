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

import com.google.api.client.http.InputStreamContent;
import com.google.api.client.util.Data;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.StorageObject;
import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;
import com.google.cloud.hadoop.gcsio.CreateFileOptions;
import com.google.cloud.hadoop.gcsio.CreateObjectOptions;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorage;
import com.google.cloud.hadoop.gcsio.GoogleCloudStorageImpl;
import com.google.cloud.hadoop.gcsio.PathCodec;
import com.google.cloud.hadoop.gcsio.StorageResourceId;
import com.google.common.collect.Maps;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.spi.connector.ConnectorSession;
import org.apache.hadoop.fs.Path;

import javax.inject.Inject;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

import static com.google.cloud.hadoop.gcsio.GoogleCloudStorageFileSystem.objectOptionsFromFileOptions;
import static io.trino.plugin.hive.util.HiveWriteUtils.getRawFileSystem;

public class GcsTransactionLogSynchronizer
        implements TransactionLogSynchronizer
{
    private static final Field STORAGE_FIELD;

    private static final Base64.Encoder base64Encoder = Base64.getEncoder();

    static {
        try {
            STORAGE_FIELD = GoogleCloudStorageImpl.class.getDeclaredField("gcs");
            STORAGE_FIELD.setAccessible(true);
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private final HdfsEnvironment hdfsEnvironment;

    @Inject
    public GcsTransactionLogSynchronizer(HdfsEnvironment hdfsEnvironment)
    {
        this.hdfsEnvironment = hdfsEnvironment;
    }

    // This approach should be compatible with OSS Delta Lake.
    @Override
    public void write(ConnectorSession session, String clusterId, Path newLogEntryPath, byte[] entryContents)
    {
        GoogleHadoopFileSystem googleHadoopFileSystem;
        try {
            googleHadoopFileSystem = (GoogleHadoopFileSystem) getRawFileSystem(hdfsEnvironment.getFileSystem(new HdfsEnvironment.HdfsContext(session), newLogEntryPath));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        Storage storage = getStorage(googleHadoopFileSystem);
        PathCodec pathCodec = googleHadoopFileSystem.getGcsFs().getOptions().getPathCodec();
        try {
            createStorageObject(newLogEntryPath.toUri(), entryContents, pathCodec, storage);
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
     * Retrieves through reflection the service definition instance for Google Cloud Storage API.
     * The {@link GoogleHadoopFileSystem} class from `hadoop-connectors` library does not expose
     * directly the {@link Storage}.
     */
    private Storage getStorage(GoogleHadoopFileSystem fs)
    {
        try {
            GoogleCloudStorage googleCloudStorage = fs.getGcsFs().getGcs();
            return (Storage) STORAGE_FIELD.get(googleCloudStorage);
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * This method uses {@link Storage} Cloud Storage API client for creating synchronously a storage object
     * with the specified path and content.
     * <p>
     * This approach avoids dealing with `FSDataOutputStream` exposed by the method
     * `org.apache.hadoop.fs.FileSystem#create(org.apache.hadoop.fs.Path)` and the intricacies of handling
     * exceptions which may occur while writing the content to the output stream.
     */
    private static StorageObject createStorageObject(URI blobPath, byte[] content, PathCodec pathCodec, Storage storage)
            throws IOException
    {
        CreateFileOptions createFileOptions = new CreateFileOptions(false);
        CreateObjectOptions createObjectOptions = objectOptionsFromFileOptions(createFileOptions);
        StorageResourceId storageResourceId = pathCodec.validatePathAndGetId(blobPath, false);

        StorageObject object =
                new StorageObject()
                        .setContentEncoding(createObjectOptions.getContentEncoding())
                        .setMetadata(encodeMetadata(createObjectOptions.getMetadata()))
                        .setName(storageResourceId.getObjectName());

        InputStream inputStream = new ByteArrayInputStream(content, 0, content.length);
        Storage.Objects.Insert insert = storage.objects().insert(
                storageResourceId.getBucketName(),
                object,
                new InputStreamContent(createObjectOptions.getContentType(), inputStream));
        // By setting `ifGenerationMatch` setting to `0`, the creation of the blob will succeed only
        // if there are no live versions of the object.
        insert.setIfGenerationMatch(0L);
        insert.getMediaHttpUploader().setDirectUploadEnabled(true);
        insert.setName(storageResourceId.getObjectName());
        return insert.execute();
    }

    /**
     * Helper for converting from a Map&lt;String, byte[]&gt; metadata map that may be in a
     * StorageObject into a Map&lt;String, String&gt; suitable for placement inside a
     * GoogleCloudStorageItemInfo.
     */
    private static Map<String, String> encodeMetadata(Map<String, byte[]> metadata)
    {
        return Maps.transformValues(metadata, GcsTransactionLogSynchronizer::encodeMetadataValues);
    }

    // A function to encode metadata map values
    private static String encodeMetadataValues(byte[] bytes)
    {
        return bytes == null ? Data.NULL_STRING : new String(base64Encoder.encode(bytes), StandardCharsets.UTF_8);
    }
}
