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
package io.trino.filesystem.gcs;

import com.google.cloud.WriteChannel;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobTargetOption;
import com.google.cloud.storage.Storage.BlobWriteOption;
import com.google.cloud.storage.StorageException;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.filesystem.encryption.EncryptionKey;
import io.trino.memory.context.AggregatedMemoryContext;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.FileAlreadyExistsException;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.filesystem.gcs.GcsUtils.encodedKey;
import static io.trino.filesystem.gcs.GcsUtils.handleGcsException;
import static java.net.HttpURLConnection.HTTP_PRECON_FAILED;
import static java.util.Objects.requireNonNull;

public class GcsOutputFile
        implements TrinoOutputFile
{
    private final GcsLocation location;
    private final Storage storage;
    private final long writeBlockSizeBytes;
    private final Optional<EncryptionKey> key;

    public GcsOutputFile(GcsLocation location, Storage storage, long writeBlockSizeBytes, Optional<EncryptionKey> key)
    {
        this.location = requireNonNull(location, "location is null");
        this.storage = requireNonNull(storage, "storage is null");
        checkArgument(writeBlockSizeBytes >= 0, "writeBlockSizeBytes is negative");
        this.writeBlockSizeBytes = writeBlockSizeBytes;
        this.key = requireNonNull(key, "key is null");
    }

    @Override
    public void createOrOverwrite(byte[] data)
            throws IOException
    {
        try {
            storage.create(blobInfo(), data, blobTargetOptions(false));
        }
        catch (RuntimeException e) {
            throw handleGcsException(e, "writing file", location);
        }
    }

    @Override
    public void createExclusive(byte[] data)
            throws IOException
    {
        try {
            storage.create(blobInfo(), data, blobTargetOptions(true));
        }
        catch (RuntimeException e) {
            throwIfAlreadyExists(e);
            throw handleGcsException(e, "writing file", location);
        }
    }

    @Override
    public OutputStream create(AggregatedMemoryContext memoryContext)
            throws IOException
    {
        try {
            WriteChannel writeChannel = storage.writer(blobInfo(), blobWriteOptions(true));
            return new GcsOutputStream(location, writeChannel, memoryContext, writeBlockSizeBytes);
        }
        catch (RuntimeException e) {
            throwIfAlreadyExists(e);
            throw handleGcsException(e, "writing file", location);
        }
    }

    @Override
    public Location location()
    {
        return location.location();
    }

    private BlobInfo blobInfo()
    {
        return BlobInfo.newBuilder(location.bucket(), location.path()).build();
    }

    private BlobWriteOption[] blobWriteOptions(boolean doesNotExist)
    {
        ImmutableCollection.Builder<BlobWriteOption> options = ImmutableList.builder();
        if (doesNotExist) {
            options.add(BlobWriteOption.doesNotExist());
        }
        key.ifPresent(encryption -> options.add(BlobWriteOption.encryptionKey(encodedKey(encryption))));
        return options.build().toArray(new BlobWriteOption[0]);
    }

    private BlobTargetOption[] blobTargetOptions(boolean doesNotExist)
    {
        ImmutableCollection.Builder<BlobTargetOption> options = ImmutableList.builder();
        if (doesNotExist) {
            options.add(BlobTargetOption.doesNotExist());
        }
        key.ifPresent(encryption -> options.add(BlobTargetOption.encryptionKey(encodedKey(encryption))));
        return options.build().toArray(new BlobTargetOption[0]);
    }

    private void throwIfAlreadyExists(RuntimeException e)
            throws FileAlreadyExistsException
    {
        // when `location` already exists, the operation will fail with `412 Precondition Failed`
        if ((e instanceof StorageException se) && (se.getCode() == HTTP_PRECON_FAILED)) {
            throw new FileAlreadyExistsException(location.toString());
        }
    }
}
