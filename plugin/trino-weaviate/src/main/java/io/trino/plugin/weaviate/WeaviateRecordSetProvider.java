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
package io.trino.plugin.weaviate;

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.type.Type;

import java.util.Collections;
import java.util.List;

import static io.trino.plugin.weaviate.WeaviateColumnHandle.toTrinoValue;
import static java.util.Collections.emptyList;

public class WeaviateRecordSetProvider
        implements ConnectorRecordSetProvider
{
    @Override
    public RecordSet getRecordSet(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit connectorSplit,
            ConnectorTableHandle connectorTable,
            List<? extends ColumnHandle> columnHandles)
    {
        WeaviateSplit split = (WeaviateSplit) connectorSplit;

        if (columnHandles.isEmpty()) {
            int count = split.values().size();
            return new InMemoryRecordSet(emptyList(), Collections.nCopies(count, emptyList()));
        }

        List<ColumnMetadata> columns = columnHandles.stream()
                .map(c -> ((WeaviateColumnHandle) c).columnMetadata())
                .toList();

        List<Type> types = columns.stream().map(ColumnMetadata::getType).toList();
        ImmutableList.Builder<List<Object>> records = ImmutableList.builder();
        for (var value : split.values()) {
            ImmutableList.Builder<Object> record = ImmutableList.builder();
            for (var i = 0; i < columns.size(); i++) {
                WeaviateColumnHandle columnHandle = (WeaviateColumnHandle) columnHandles.get(i);
                Object rawValue = value.get(columnHandle.name());
                Object trinoValue = toTrinoValue(rawValue, columnHandle.trinoType());
                record.add(trinoValue);
            }
            records.add(record.build());
        }
        return new InMemoryRecordSet(types, records.build());
    }
}
