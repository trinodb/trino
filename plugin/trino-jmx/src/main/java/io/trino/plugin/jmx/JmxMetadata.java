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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Streams;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.TableColumnsMetadata;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.Type;

import javax.management.JMException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.jmx.JmxErrorCode.JMX_INVALID_TABLE_NAME;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.util.Collections.emptyIterator;
import static java.util.Comparator.comparing;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static javax.management.ObjectName.WILDCARD;

public class JmxMetadata
        implements ConnectorMetadata
{
    private static final Logger LOGGER = Logger.get(JmxMetadata.class);
    public static final String JMX_SCHEMA_NAME = "current";
    public static final String HISTORY_SCHEMA_NAME = "history";
    public static final String NODE_COLUMN_NAME = "node";
    public static final String OBJECT_NAME_NAME = "object_name";
    public static final String TIMESTAMP_COLUMN_NAME = "timestamp";

    private final MBeanServer mbeanServer;
    private final JmxHistoricalData jmxHistoricalData;

    @Inject
    public JmxMetadata(MBeanServer mbeanServer, JmxHistoricalData jmxHistoricalData)
    {
        this.mbeanServer = requireNonNull(mbeanServer, "mbeanServer is null");
        this.jmxHistoricalData = requireNonNull(jmxHistoricalData, "jmxHistoricalData is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.of(JMX_SCHEMA_NAME, HISTORY_SCHEMA_NAME);
    }

    @Override
    public JmxTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName, Optional<ConnectorTableVersion> startVersion, Optional<ConnectorTableVersion> endVersion)
    {
        if (startVersion.isPresent() || endVersion.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support versioned tables");
        }

        return getTableHandle(tableName);
    }

    public JmxTableHandle getTableHandle(SchemaTableName tableName)
    {
        requireNonNull(tableName, "tableName is null");
        if (tableName.getSchemaName().equals(JMX_SCHEMA_NAME)) {
            return getJmxTableHandle(tableName);
        }
        if (tableName.getSchemaName().equals(HISTORY_SCHEMA_NAME)) {
            return getJmxHistoryTableHandle(tableName);
        }
        return null;
    }

    private JmxTableHandle getJmxHistoryTableHandle(SchemaTableName tableName)
    {
        JmxTableHandle handle = getJmxTableHandle(tableName);
        if (handle == null) {
            return null;
        }
        ImmutableList.Builder<JmxColumnHandle> builder = ImmutableList.builder();
        builder.add(new JmxColumnHandle(TIMESTAMP_COLUMN_NAME, createTimestampWithTimeZoneType(3)));
        builder.addAll(handle.columnHandles());
        return new JmxTableHandle(handle.tableName(), handle.objectNames(), builder.build(), false, TupleDomain.all());
    }

    private JmxTableHandle getJmxTableHandle(SchemaTableName tableName)
    {
        try {
            String objectNamePattern = toPattern(tableName.getTableName().toLowerCase(ENGLISH));
            List<ObjectName> objectNames = mbeanServer.queryNames(WILDCARD, null).stream()
                    .filter(name -> name.getCanonicalName().toLowerCase(ENGLISH).matches(objectNamePattern))
                    .collect(toImmutableList());
            if (objectNames.isEmpty()) {
                return null;
            }
            List<JmxColumnHandle> columns = new ArrayList<>();
            columns.add(new JmxColumnHandle(NODE_COLUMN_NAME, createUnboundedVarcharType()));
            columns.add(new JmxColumnHandle(OBJECT_NAME_NAME, createUnboundedVarcharType()));
            for (ObjectName objectName : objectNames) {
                MBeanInfo mbeanInfo = mbeanServer.getMBeanInfo(objectName);

                getColumnHandles(mbeanInfo).forEach(columns::add);
            }

            // Since this method is being called on all nodes in the cluster, we must ensure (by sorting)
            // that attributes are in the same order on all of them.
            columns = columns.stream()
                    .distinct()
                    .sorted(comparing(JmxColumnHandle::columnName))
                    .collect(toImmutableList());

            return new JmxTableHandle(tableName, objectNames.stream().map(ObjectName::toString).collect(toImmutableList()), columns, true, TupleDomain.all());
        }
        catch (JMException | TrinoException e) {
            return null;
        }
    }

    public static String toPattern(String tableName)
    {
        try {
            if (!tableName.contains("*")) {
                return Pattern.quote(new ObjectName(tableName).getCanonicalName());
            }
            return Streams.stream(Splitter.on('*').split(tableName))
                    .map(Pattern::quote)
                    .collect(Collectors.joining(".*"));
        }
        catch (MalformedObjectNameException exception) {
            LOGGER.debug(exception, "Invalid ObjectName");
            throw new TrinoException(JMX_INVALID_TABLE_NAME, "Not a valid ObjectName " + tableName);
        }
    }

    private Stream<JmxColumnHandle> getColumnHandles(MBeanInfo mbeanInfo)
    {
        return Arrays.stream(mbeanInfo.getAttributes())
                .filter(MBeanAttributeInfo::isReadable)
                .map(attribute -> new JmxColumnHandle(attribute.getName(), getColumnType(attribute)));
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return ((JmxTableHandle) tableHandle).getTableMetadata();
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        Set<String> schemaNames = schemaName.map(ImmutableSet::of)
                .orElseGet(() -> ImmutableSet.copyOf(listSchemaNames(session)));
        ImmutableList.Builder<SchemaTableName> schemaTableNames = ImmutableList.builder();
        for (String schema : schemaNames) {
            if (JMX_SCHEMA_NAME.equals(schema)) {
                return listJmxTables();
            }
            if (HISTORY_SCHEMA_NAME.equals(schema)) {
                return jmxHistoricalData.getTables().stream()
                        .map(tableName -> new SchemaTableName(JmxMetadata.HISTORY_SCHEMA_NAME, tableName))
                        .collect(toList());
            }
        }
        return schemaTableNames.build();
    }

    private List<SchemaTableName> listJmxTables()
    {
        ImmutableList.Builder<SchemaTableName> tableNames = ImmutableList.builder();
        for (ObjectName objectName : mbeanServer.queryNames(WILDCARD, null)) {
            // todo remove lower case when Trino supports mixed case names
            tableNames.add(new SchemaTableName(JMX_SCHEMA_NAME, objectName.getCanonicalName().toLowerCase(ENGLISH)));
        }
        return tableNames.build();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        JmxTableHandle jmxTableHandle = (JmxTableHandle) tableHandle;
        return ImmutableMap.copyOf(Maps.uniqueIndex(jmxTableHandle.columnHandles(), column -> column.columnName().toLowerCase(ENGLISH)));
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((JmxColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        throw new UnsupportedOperationException("The deprecated listTableColumns is not supported because streamTableColumns is implemented instead");
    }

    @Override
    public Iterator<TableColumnsMetadata> streamTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        if (prefix.getSchema().isPresent() &&
                !prefix.getSchema().get().equals(JMX_SCHEMA_NAME) &&
                !prefix.getSchema().get().equals(HISTORY_SCHEMA_NAME)) {
            return emptyIterator();
        }

        List<SchemaTableName> tableNames;
        if (prefix.getTable().isEmpty()) {
            tableNames = listTables(session, prefix.getSchema());
        }
        else {
            tableNames = ImmutableList.of(prefix.toSchemaTableName());
        }

        return tableNames.stream()
                .map(tableName -> TableColumnsMetadata.forTable(tableName, getTableHandle(session, tableName, Optional.empty(), Optional.empty()).getTableMetadata().getColumns()))
                .iterator();
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle handle, Constraint constraint)
    {
        Map<ColumnHandle, Domain> domains = constraint.getSummary().getDomains().orElseThrow(() -> new IllegalArgumentException("constraint summary is NONE"));

        JmxTableHandle tableHandle = (JmxTableHandle) handle;

        Map<ColumnHandle, Domain> nodeDomains = new LinkedHashMap<>();
        Map<ColumnHandle, Domain> otherDomains = new LinkedHashMap<>();
        domains.forEach((column, domain) -> {
            JmxColumnHandle columnHandle = (JmxColumnHandle) column;
            if (columnHandle.columnName().equals(NODE_COLUMN_NAME)) {
                nodeDomains.put(column, domain);
            }
            else {
                otherDomains.put(column, domain);
            }
        });

        TupleDomain<ColumnHandle> oldDomain = tableHandle.nodeFilter();
        TupleDomain<ColumnHandle> newDomain = oldDomain.intersect(TupleDomain.withColumnDomains(nodeDomains));

        if (oldDomain.equals(newDomain)) {
            return Optional.empty();
        }

        JmxTableHandle newTableHandle = new JmxTableHandle(tableHandle.tableName(), tableHandle.objectNames(), tableHandle.columnHandles(), tableHandle.liveData(), newDomain);

        return Optional.of(new ConstraintApplicationResult<>(newTableHandle, TupleDomain.withColumnDomains(otherDomains), constraint.getExpression(), false));
    }

    private static Type getColumnType(MBeanAttributeInfo attribute)
    {
        return switch (attribute.getType()) {
            case "boolean", "java.lang.Boolean" -> BOOLEAN;
            case "byte", "java.lang.Byte", "short", "java.lang.Short", "int", "java.lang.Integer", "long", "java.lang.Long" -> BIGINT;
            case "java.lang.Number", "float", "java.lang.Float", "double", "java.lang.Double" -> DOUBLE;
            default -> createUnboundedVarcharType();
        };
    }
}
