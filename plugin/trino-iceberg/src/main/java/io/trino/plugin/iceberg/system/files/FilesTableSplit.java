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
package io.trino.plugin.iceberg.system.files;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.airlift.slice.SizeOf;
import io.trino.plugin.iceberg.bean.ManifestFileBean;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.type.Type;
import org.apache.iceberg.ManifestFile;

import java.util.Map;

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;

public record FilesTableSplit(
        @JsonDeserialize(as = ManifestFileBean.class) ManifestFile manifestFile,
        String schemaJson,
        String metadataTableJson,
        Map<Integer, String> partitionSpecsByIdJson,
        Map<String, Type> columns,
        Map<String, String> fileIoProperties)
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = instanceSize(FilesTableSplit.class);

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + ManifestFileBean.INSTANCE_SIZE
                + estimatedSizeOf(schemaJson)
                + estimatedSizeOf(metadataTableJson)
                + estimatedSizeOf(partitionSpecsByIdJson, SizeOf::sizeOf, SizeOf::estimatedSizeOf)
                + estimatedSizeOf(columns, SizeOf::estimatedSizeOf, (t) -> instanceSize(t.getClass()))
                + estimatedSizeOf(fileIoProperties, SizeOf::estimatedSizeOf, SizeOf::estimatedSizeOf);
    }
}
