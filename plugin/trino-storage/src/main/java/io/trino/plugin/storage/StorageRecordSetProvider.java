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
package io.trino.plugin.storage;

import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.storage.functions.ReadCsvTableFunction.ReadCsvTableHandle;
import io.trino.plugin.storage.reader.CsvFileReader;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.type.Type;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class StorageRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final TrinoFileSystemFactory fileSystemFactory;

    @Inject
    public StorageRecordSetProvider(TrinoFileSystemFactory fileSystemFactory)
    {
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
    }

    @Override
    public RecordSet getRecordSet(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<? extends ColumnHandle> columns)
    {
        requireNonNull(split, "split is null");

        StorageSplit storageSplit = (StorageSplit) split;
        String location = storageSplit.location();
        TrinoFileSystem fileSystem = fileSystemFactory.create(session);

        Stream<List<?>> stream;
        List<StorageColumnHandle> tableColumns;
        if (table instanceof ReadCsvTableHandle readCsv) {
            CsvFileReader csvfileReader = new CsvFileReader(readCsv.separator());
            stream = csvfileReader.getRecordsIterator(location, path -> {
                try {
                    return fileSystem.newInputFile(Location.of(path)).newStream();
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            tableColumns = readCsv.columns();
        }
        else {
            throw new IllegalArgumentException("Unknown table handle: " + table);
        }

        Iterable<List<?>> rows = stream::iterator;

        List<StorageColumnHandle> handles = columns.stream()
                .map(column -> (StorageColumnHandle) column)
                .toList();
        List<Integer> columnIndexes = handles.stream()
                .map(column -> {
                    int index = 0;
                    for (StorageColumnHandle columnHandle : tableColumns) {
                        if (columnHandle.name().equalsIgnoreCase(column.name())) {
                            return index;
                        }
                        index++;
                    }
                    throw new IllegalStateException("Unknown column: " + column.name());
                })
                .toList();

        //noinspection StaticPseudoFunctionalStyleMethod
        Iterable<List<?>> mappedRows = Iterables.transform(rows, row -> columnIndexes.stream()
                .map(row::get)
                .collect(toList()));

        List<Type> mappedTypes = handles.stream()
                .map(StorageColumnHandle::type)
                .collect(toList());
        return new InMemoryRecordSet(mappedTypes, mappedRows);
    }
}
