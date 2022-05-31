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

package $package;

import com.google.common.collect.Iterables;
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

import javax.inject.Inject;

import java.util.List;

import static java.util.stream.Collectors.toList;

public class ${classPrefix}RecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final ${classPrefix}Config config;
    private final ${classPrefix}Metadata metadata;

    @Inject
    public ${classPrefix}RecordSetProvider(${classPrefix}Config config, ${classPrefix}Metadata metadata)
    {
        this.config = config;
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
        // TODO implement
        Iterable<List<?>> rows = List.of(
                List.of("x", config.getSomeSetting(), "my-name"));

        List<${classPrefix}ColumnHandle> restColumnHandles = list
                .stream()
                .map(c -> (${classPrefix}ColumnHandle) c)
                .collect(toList());
        ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(connectorSession, table);

        List<Integer> columnIndexes = restColumnHandles
                .stream()
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

        //noinspection StaticPseudoFunctionalStyleMethod
        Iterable<List<?>> mappedRows = Iterables.transform(rows, row -> columnIndexes
                .stream()
                .map(row::get)
                .collect(toList()));

        List<Type> mappedTypes = restColumnHandles
                .stream()
                .map(${classPrefix}ColumnHandle::getType)
                .collect(toList());
        return new InMemoryRecordSet(mappedTypes, mappedRows);
    }
}
