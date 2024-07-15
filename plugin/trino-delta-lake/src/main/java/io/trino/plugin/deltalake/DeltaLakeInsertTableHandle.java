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
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.deltalake.transactionlog.MetadataEntry;
import io.trino.plugin.deltalake.transactionlog.ProtocolEntry;
import io.trino.spi.connector.ConnectorInsertTableHandle;
import io.trino.spi.connector.SchemaTableName;

import java.util.List;

import static java.util.Objects.requireNonNull;

public record DeltaLakeInsertTableHandle(
        SchemaTableName tableName,
        String location,
        MetadataEntry metadataEntry,
        ProtocolEntry protocolEntry,
        List<DeltaLakeColumnHandle> inputColumns,
        long readVersion,
        boolean retriesEnabled)
        implements ConnectorInsertTableHandle
{
    public DeltaLakeInsertTableHandle
    {
        requireNonNull(tableName, "tableName is null");
        requireNonNull(metadataEntry, "metadataEntry is null");
        requireNonNull(protocolEntry, "protocolEntry is null");
        inputColumns = ImmutableList.copyOf(requireNonNull(inputColumns, "inputColumns is null"));
        requireNonNull(location, "location is null");
    }

    @Override
    public String toString()
    {
        return tableName + "[" + location + "]";
    }
}
