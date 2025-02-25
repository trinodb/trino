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
package io.trino.testing.tpch;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import io.airlift.slice.Slice;
import io.trino.plugin.tpch.DecimalTypeMapping;
import io.trino.plugin.tpch.TpchMetadata;
import io.trino.plugin.tpch.TpchTableHandle;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;
import io.trino.tpch.TpchTable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkPositionIndex;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.tpch.TpchRecordSet.getRecordSet;
import static java.util.Objects.requireNonNull;

public class TpchIndexedData
{
    private final Map<Set<TpchScaledColumn>, IndexedTable> indexedTables;

    public TpchIndexedData(TpchIndexSpec tpchIndexSpec)
    {
        requireNonNull(tpchIndexSpec, "tpchIndexSpec is null");

        TpchMetadata tpchMetadata = new TpchMetadata();

        ImmutableMap.Builder<Set<TpchScaledColumn>, IndexedTable> indexedTablesBuilder = ImmutableMap.builder();

        Set<TpchScaledTable> tables = tpchIndexSpec.listIndexedTables();
        for (TpchScaledTable table : tables) {
            SchemaTableName tableName = new SchemaTableName("sf" + table.getScaleFactor(), table.getTableName());
            TpchTableHandle tableHandle = tpchMetadata.getTableHandle(null, tableName, Optional.empty(), Optional.empty());
            Map<String, ColumnHandle> columnHandles = new LinkedHashMap<>(tpchMetadata.getColumnHandles(null, tableHandle));
            for (Set<String> columnNames : tpchIndexSpec.getColumnIndexes(table)) {
                List<String> keyColumnNames = ImmutableList.copyOf(columnNames); // Finalize the key order
                Set<TpchScaledColumn> keyColumns = keyColumnNames.stream()
                        .map(name -> new TpchScaledColumn(table, name))
                        .collect(toImmutableSet());

                TpchTable<?> tpchTable = TpchTable.getTable(table.getTableName());
                RecordSet recordSet = getRecordSet(tpchTable, ImmutableList.copyOf(columnHandles.values()), table.getScaleFactor(), 0, 1, TupleDomain.all(), DecimalTypeMapping.DOUBLE);
                IndexedTable indexedTable = indexTable(recordSet, ImmutableList.copyOf(columnHandles.keySet()), keyColumnNames);
                indexedTablesBuilder.put(keyColumns, indexedTable);
            }
        }

        indexedTables = indexedTablesBuilder.buildOrThrow();
    }

    public Optional<IndexedTable> getIndexedTable(String tableName, double scaleFactor, Set<String> indexColumnNames)
    {
        TpchScaledTable table = new TpchScaledTable(tableName, scaleFactor);
        Set<TpchScaledColumn> indexColumns = indexColumnNames.stream()
                .map(name -> new TpchScaledColumn(table, name))
                .collect(toImmutableSet());
        return Optional.ofNullable(indexedTables.get(indexColumns));
    }

    private static <T> List<T> extractPositionValues(List<T> values, List<Integer> positions)
    {
        return Lists.transform(positions, position -> {
            checkPositionIndex(position, values.size());
            return values.get(position);
        });
    }

    private static IndexedTable indexTable(RecordSet recordSet, List<String> outputColumns, List<String> keyColumns)
    {
        List<Integer> keyPositions = keyColumns.stream()
                .map(columnName -> {
                    int position = outputColumns.indexOf(columnName);
                    checkState(position != -1);
                    return position;
                })
                .collect(toImmutableList());

        ImmutableListMultimap.Builder<MaterializedTuple, MaterializedTuple> indexedValuesBuilder = ImmutableListMultimap.builder();

        List<Type> outputTypes = recordSet.getColumnTypes();
        List<Type> keyTypes = extractPositionValues(outputTypes, keyPositions);

        RecordCursor cursor = recordSet.cursor();
        while (cursor.advanceNextPosition()) {
            List<Object> values = extractValues(cursor, outputTypes);
            List<Object> keyValues = extractPositionValues(values, keyPositions);

            indexedValuesBuilder.put(new MaterializedTuple(keyValues), new MaterializedTuple(values));
        }

        return new IndexedTable(keyColumns, keyTypes, outputColumns, outputTypes, indexedValuesBuilder.build());
    }

    private static List<Object> extractValues(RecordCursor cursor, List<Type> types)
    {
        List<Object> list = new ArrayList<>(types.size());
        for (int i = 0; i < types.size(); i++) {
            list.add(extractObject(cursor, i, types.get(i)));
        }
        return list;
    }

    private static Object extractObject(RecordCursor cursor, int field, Type type)
    {
        if (cursor.isNull(field)) {
            return null;
        }

        Class<?> javaType = type.getJavaType();
        if (javaType == boolean.class) {
            return cursor.getBoolean(field);
        }
        if (javaType == long.class) {
            return cursor.getLong(field);
        }
        if (javaType == double.class) {
            return cursor.getDouble(field);
        }
        if (javaType == Slice.class) {
            return cursor.getSlice(field).toStringUtf8();
        }
        throw new AssertionError("Unsupported type: " + type);
    }

    public static class IndexedTable
    {
        private final List<String> keyColumnNames;
        private final List<Type> keyTypes;
        private final List<String> outputColumnNames;
        private final List<Type> outputTypes;
        private final ListMultimap<MaterializedTuple, MaterializedTuple> keyToValues;

        private IndexedTable(List<String> keyColumnNames, List<Type> keyTypes, List<String> outputColumnNames, List<Type> outputTypes, ListMultimap<MaterializedTuple, MaterializedTuple> keyToValues)
        {
            this.keyColumnNames = ImmutableList.copyOf(requireNonNull(keyColumnNames, "keyColumnNames is null"));
            this.keyTypes = ImmutableList.copyOf(requireNonNull(keyTypes, "keyTypes is null"));
            this.outputColumnNames = ImmutableList.copyOf(requireNonNull(outputColumnNames, "outputColumnNames is null"));
            this.outputTypes = ImmutableList.copyOf(requireNonNull(outputTypes, "outputTypes is null"));
            this.keyToValues = ImmutableListMultimap.copyOf(requireNonNull(keyToValues, "keyToValues is null"));
        }

        public List<String> getKeyColumns()
        {
            return keyColumnNames;
        }

        public List<String> getOutputColumns()
        {
            return outputColumnNames;
        }

        public RecordSet lookupKeys(RecordSet recordSet)
        {
            checkArgument(recordSet.getColumnTypes().equals(keyTypes), "Input RecordSet keys do not match expected key type");

            Iterable<RecordSet> outputRecordSets = Iterables.transform(tupleIterable(recordSet), key -> {
                for (Object value : key.getValues()) {
                    if (value == null) {
                        throw new IllegalArgumentException("TPCH index does not support null values");
                    }
                }
                return lookupKey(key);
            });

            return new ConcatRecordSet(outputRecordSets, outputTypes);
        }

        public RecordSet lookupKey(MaterializedTuple tupleKey)
        {
            return new MaterializedTupleRecordSet(keyToValues.get(tupleKey), outputTypes);
        }

        private static Iterable<MaterializedTuple> tupleIterable(RecordSet recordSet)
        {
            return () -> new AbstractIterator<>()
            {
                private final RecordCursor cursor = recordSet.cursor();

                @Override
                protected MaterializedTuple computeNext()
                {
                    if (!cursor.advanceNextPosition()) {
                        return endOfData();
                    }
                    return new MaterializedTuple(extractValues(cursor, recordSet.getColumnTypes()));
                }
            };
        }
    }

    private static class TpchScaledColumn
    {
        private final TpchScaledTable table;
        private final String columnName;

        private TpchScaledColumn(TpchScaledTable table, String columnName)
        {
            this.table = requireNonNull(table, "table is null");
            this.columnName = requireNonNull(columnName, "columnName is null");
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(table, columnName);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            TpchScaledColumn other = (TpchScaledColumn) obj;
            return Objects.equals(this.table, other.table) && Objects.equals(this.columnName, other.columnName);
        }
    }
}
