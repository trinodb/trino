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
package io.trino.plugin.jmx;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.slice.Slice;
import io.trino.spi.NodeManager;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.InMemoryRecordSet;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.type.Type;

import javax.management.Attribute;
import javax.management.InstanceNotFoundException;
import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static java.util.Objects.requireNonNull;

public class JmxRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final MBeanServer mbeanServer;
    private final String nodeId;
    private final JmxHistoricalData jmxHistoricalData;

    @Inject
    public JmxRecordSetProvider(MBeanServer mbeanServer, NodeManager nodeManager, JmxHistoricalData jmxHistoricalData)
    {
        this.mbeanServer = requireNonNull(mbeanServer, "mbeanServer is null");
        this.nodeId = nodeManager.getCurrentNode().getNodeIdentifier();
        this.jmxHistoricalData = requireNonNull(jmxHistoricalData, "jmxHistoricalData is null");
    }

    public List<Object> getLiveRow(String objectName, List<? extends ColumnHandle> columns, long entryTimestamp)
            throws JMException
    {
        ImmutableMap<String, Optional<Object>> attributes = getAttributes(getColumnNames(columns), objectName);
        List<Object> row = new ArrayList<>();

        for (ColumnHandle column : columns) {
            JmxColumnHandle jmxColumn = (JmxColumnHandle) column;
            if (jmxColumn.columnName().equals(JmxMetadata.NODE_COLUMN_NAME)) {
                row.add(nodeId);
            }
            else if (jmxColumn.columnName().equals(JmxMetadata.OBJECT_NAME_NAME)) {
                row.add(objectName);
            }
            else if (jmxColumn.columnName().equals(JmxMetadata.TIMESTAMP_COLUMN_NAME)) {
                row.add(packDateTimeWithZone(entryTimestamp, UTC_KEY));
            }
            else {
                Optional<Object> optionalValue = attributes.get(jmxColumn.columnName());
                if (optionalValue == null || optionalValue.isEmpty()) {
                    row.add(null);
                }
                else {
                    Object value = optionalValue.get();
                    Class<?> javaType = jmxColumn.columnType().getJavaType();
                    if (javaType == boolean.class) {
                        if (value instanceof Boolean) {
                            row.add(value);
                        }
                        else {
                            // mbeans can lie about types
                            row.add(null);
                        }
                    }
                    else if (javaType == long.class) {
                        if (value instanceof Number) {
                            row.add(((Number) value).longValue());
                        }
                        else {
                            // mbeans can lie about types
                            row.add(null);
                        }
                    }
                    else if (javaType == double.class) {
                        if (value instanceof Number) {
                            row.add(((Number) value).doubleValue());
                        }
                        else {
                            // mbeans can lie about types
                            row.add(null);
                        }
                    }
                    else if (javaType == Slice.class) {
                        if (value.getClass().isArray()) {
                            // return a string representation of the array
                            if (value.getClass().getComponentType() == boolean.class) {
                                row.add(Arrays.toString((boolean[]) value));
                            }
                            else if (value.getClass().getComponentType() == byte.class) {
                                row.add(Arrays.toString((byte[]) value));
                            }
                            else if (value.getClass().getComponentType() == char.class) {
                                row.add(Arrays.toString((char[]) value));
                            }
                            else if (value.getClass().getComponentType() == double.class) {
                                row.add(Arrays.toString((double[]) value));
                            }
                            else if (value.getClass().getComponentType() == float.class) {
                                row.add(Arrays.toString((float[]) value));
                            }
                            else if (value.getClass().getComponentType() == int.class) {
                                row.add(Arrays.toString((int[]) value));
                            }
                            else if (value.getClass().getComponentType() == long.class) {
                                row.add(Arrays.toString((long[]) value));
                            }
                            else if (value.getClass().getComponentType() == short.class) {
                                row.add(Arrays.toString((short[]) value));
                            }
                            else {
                                row.add(Arrays.toString((Object[]) value));
                            }
                        }
                        else {
                            row.add(value.toString());
                        }
                    }
                }
            }
        }
        return row;
    }

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transaction, ConnectorSession session, ConnectorSplit split, ConnectorTableHandle table, List<? extends ColumnHandle> columns)
    {
        JmxTableHandle tableHandle = (JmxTableHandle) table;

        requireNonNull(columns, "columns is null");
        checkArgument(!columns.isEmpty(), "must provide at least one column");

        List<List<Object>> rows;
        try {
            if (tableHandle.liveData()) {
                rows = getLiveRows(tableHandle, columns);
            }
            else {
                List<Integer> selectedColumns = calculateSelectedColumns(tableHandle.columnHandles(), getColumnNames(columns));
                rows = tableHandle.objectNames().stream()
                        .flatMap(objectName -> jmxHistoricalData.getRows(objectName, selectedColumns).stream())
                        .collect(toImmutableList());
            }
        }
        catch (JMException e) {
            rows = ImmutableList.of();
        }

        return new InMemoryRecordSet(getColumnTypes(columns), rows);
    }

    private List<Integer> calculateSelectedColumns(List<JmxColumnHandle> columnHandles, Set<String> selectedColumnNames)
    {
        ImmutableList.Builder<Integer> selectedColumns = ImmutableList.builder();
        for (int i = 0; i < columnHandles.size(); i++) {
            JmxColumnHandle column = columnHandles.get(i);
            if (selectedColumnNames.contains(column.columnName())) {
                selectedColumns.add(i);
            }
        }
        return selectedColumns.build();
    }

    private static Set<String> getColumnNames(List<? extends ColumnHandle> columnHandles)
    {
        return columnHandles.stream()
                .map(column -> (JmxColumnHandle) column)
                .map(JmxColumnHandle::columnName)
                .collect(Collectors.toSet());
    }

    private static List<Type> getColumnTypes(List<? extends ColumnHandle> columnHandles)
    {
        return columnHandles.stream()
                .map(column -> (JmxColumnHandle) column)
                .map(JmxColumnHandle::columnType)
                .collect(Collectors.toList());
    }

    private ImmutableMap<String, Optional<Object>> getAttributes(Set<String> uniqueColumnNames, String name)
            throws JMException
    {
        ObjectName objectName = new ObjectName(name);

        String[] columnNamesArray = uniqueColumnNames.toArray(new String[uniqueColumnNames.size()]);

        ImmutableMap.Builder<String, Optional<Object>> attributes = ImmutableMap.builder();
        for (Attribute attribute : mbeanServer.getAttributes(objectName, columnNamesArray).asList()) {
            attributes.put(attribute.getName(), Optional.ofNullable(attribute.getValue()));
        }
        return attributes.buildOrThrow();
    }

    private List<List<Object>> getLiveRows(JmxTableHandle tableHandle, List<? extends ColumnHandle> columns)
            throws JMException
    {
        ImmutableList.Builder<List<Object>> rows = ImmutableList.builder();
        for (String objectName : tableHandle.objectNames()) {
            try {
                rows.add(getLiveRow(objectName, columns, 0));
            }
            catch (InstanceNotFoundException _) {
                // Ignore if the object doesn't exist. This might happen when it exists on the coordinator but has not yet been created on the worker.
            }
        }
        return rows.build();
    }
}
