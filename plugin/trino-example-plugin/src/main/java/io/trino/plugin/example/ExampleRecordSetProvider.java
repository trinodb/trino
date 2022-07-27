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

package io.trino.plugin.example;

import com.google.inject.Inject;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.toList;

public class ExampleRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final String defaultType;
    private final ExampleMetadata metadata;

    @Inject
    public ExampleRecordSetProvider(ExampleConfig config, ExampleMetadata metadata)
    {
        this.defaultType = config.getDefaultType();
        this.metadata = metadata;
    }

    @Override
    public RecordSet getRecordSet(
            ConnectorTransactionHandle connectorTransactionHandle,
            ConnectorSession connectorSession,
            ConnectorSplit connectorSplit,
            ConnectorTableHandle table,
            List<? extends ColumnHandle> list)
    {
        List<ExampleColumnHandle> columnHandles = list.stream()
                .map(c -> (ExampleColumnHandle) c)
                .collect(toList());
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(connectorSession, table);

        List<Integer> columnIndexes = columnHandles.stream()
                .map(column -> {
                    int index = 0;
                    for (ColumnMetadata columnMetadata : tableMetadata.getColumns()) {
                        if (columnMetadata.getName().equalsIgnoreCase(column.getName())) {
                            return index;
                        }
                        index++;
                    }
                    throw new IllegalStateException("Unknown column: " + column.getName());
                })
                .collect(toList());

        Iterable<List<?>> rows = getRows();
        Iterable<List<?>> mappedRows = StreamSupport.stream(rows.spliterator(), false)
                .map(row -> columnIndexes.stream()
                        .map(row::get)
                        .collect(toList())).collect(toList());

        List<Type> mappedTypes = columnHandles.stream()
                .map(ExampleColumnHandle::getType)
                .collect(toList());
        return new InMemoryRecordSet(mappedTypes, mappedRows);
    }

    private Iterable<List<?>> getRows()
    {
        // TODO replace the list with an iterable that provides the data read from the data source for this connector
        return List.of(
                List.of("x", defaultType, "my-name"));
    }
}
