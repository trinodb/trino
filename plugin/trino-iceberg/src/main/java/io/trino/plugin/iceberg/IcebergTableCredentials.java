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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableMap;
import io.airlift.slice.SizeOf;
import io.trino.spi.connector.ConnectorTableCredentials;
import org.apache.iceberg.io.FileIO;

import java.util.Map;

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;

public record IcebergTableCredentials(Map<String, String> fileIoProperties)
        implements ConnectorTableCredentials
{
    private static final int INSTANCE_SIZE = instanceSize(IcebergTableCredentials.class);

    public IcebergTableCredentials
    {
        fileIoProperties = ImmutableMap.copyOf(fileIoProperties);
    }

    public static IcebergTableCredentials forFileIO(FileIO io)
    {
        return new IcebergTableCredentials(io.properties());
    }

    public long retainedSizeInBytes()
    {
        return INSTANCE_SIZE + estimatedSizeOf(fileIoProperties, SizeOf::estimatedSizeOf, SizeOf::estimatedSizeOf);
    }
}
