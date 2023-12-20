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
import io.trino.memory.context.AggregatedMemoryContext;
import software.amazon.awssdk.services.s3.S3Client;

import java.io.OutputStream;

import static java.util.Objects.requireNonNull;

final class S3OutputFile
        implements TrinoOutputFile
{
    private final S3Client client;
    private final S3Context context;
    private final S3Location location;

    public S3OutputFile(S3Client client, S3Context context, S3Location location)
    {
        this.client = requireNonNull(client, "client is null");
        this.context = requireNonNull(context, "context is null");
        this.location = requireNonNull(location, "location is null");
        location.location().verifyValidFileLocation();
    }

    @Override
    public OutputStream create(AggregatedMemoryContext memoryContext)
    {
        // always overwrite since Trino usually creates unique file names
        return createOrOverwrite(memoryContext);
    }

    @Override
    public OutputStream createOrOverwrite(AggregatedMemoryContext memoryContext)
    {
        return new S3OutputStream(memoryContext, client, context, location);
    }

    @Override
    public Location location()
    {
        return location.location();
    }
}
