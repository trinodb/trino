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
package io.trino.plugin.iceberg.system.entries;

import io.airlift.slice.SizeOf;
import io.trino.plugin.iceberg.system.files.TrinoManifestFile;
import io.trino.spi.connector.ConnectorSplit;

import java.util.Map;

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;

/**
 * One {@code $entries}/{@code $all_entries} split = one manifest. Carries a lightweight {@link TrinoManifestFile}
 * descriptor (not a {@code GenericManifestFile}) plus the JSON needed to reconstruct the schema and partition specs
 * on the worker, mirroring {@link io.trino.plugin.iceberg.system.files.FilesTableSplit}.
 */
public record EntriesTableSplit(
        TrinoManifestFile manifestFile,
        String schemaJson,
        String metadataSchemaJson,
        Map<Integer, String> partitionSpecsByIdJson)
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = instanceSize(EntriesTableSplit.class);

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + manifestFile.getRetainedSizeInBytes()
                + estimatedSizeOf(schemaJson)
                + estimatedSizeOf(metadataSchemaJson)
                + estimatedSizeOf(partitionSpecsByIdJson, SizeOf::sizeOf, SizeOf::estimatedSizeOf);
    }
}
