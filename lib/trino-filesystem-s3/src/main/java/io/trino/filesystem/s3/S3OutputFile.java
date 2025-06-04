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
package io.trino.filesystem.s3;

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.filesystem.encryption.EncryptionKey;
import io.trino.memory.context.AggregatedMemoryContext;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Optional;
import java.util.concurrent.Executor;

import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static java.util.Objects.requireNonNull;

final class S3OutputFile
        implements TrinoOutputFile
{
    private final Executor uploadExecutor;
    private final S3Client client;
    private final S3Context context;
    private final S3Location location;
    private final Optional<EncryptionKey> key;

    public S3OutputFile(Executor uploadExecutor, S3Client client, S3Context context, S3Location location, Optional<EncryptionKey> key)
    {
        this.uploadExecutor = requireNonNull(uploadExecutor, "uploadExecutor is null");
        this.client = requireNonNull(client, "client is null");
        this.context = requireNonNull(context, "context is null");
        this.location = requireNonNull(location, "location is null");
        this.key = requireNonNull(key, "key is null");
        location.location().verifyValidFileLocation();
    }

    @Override
    public void createOrOverwrite(byte[] data)
            throws IOException
    {
        try (OutputStream out = create(newSimpleAggregatedMemoryContext(), false)) {
            out.write(data);
        }
    }

    @Override
    public void createExclusive(byte[] data)
            throws IOException
    {
        if (!context.exclusiveWriteSupported()) {
            throw new UnsupportedOperationException("createExclusive not supported by " + getClass());
        }

        try (OutputStream out = create(newSimpleAggregatedMemoryContext(), true)) {
            out.write(data);
        }
    }

    @Override
    public OutputStream create(AggregatedMemoryContext memoryContext)
    {
        return create(memoryContext, context.exclusiveWriteSupported());
    }

    public OutputStream create(AggregatedMemoryContext memoryContext, boolean exclusive)
    {
        return new S3OutputStream(memoryContext, uploadExecutor, client, context, location, exclusive, key);
    }

    @Override
    public Location location()
    {
        return location.location();
    }
}
