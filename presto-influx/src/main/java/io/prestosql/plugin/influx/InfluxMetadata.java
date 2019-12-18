package io.prestosql.plugin.influx;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.connector.*;

import javax.inject.Inject;
import java.util.*;

import static java.util.Objects.requireNonNull;

public class InfluxMetadata implements ConnectorMetadata {

    private InfluxClient client;

    @Inject
    public InfluxMetadata(InfluxClient client) {
        this.client = requireNonNull(client, "client is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        return ImmutableList.copyOf(client.getRetentionPolicies().keySet());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName) {
        Collection<String> retentionPolicies;
        if (schemaName.isPresent()) {
            if (client.getRetentionPolicies().containsKey(schemaName.get())) {
                retentionPolicies = Collections.singletonList(schemaName.get());
            } else {
                return Collections.emptyList();
            }
        } else {
            retentionPolicies = client.getRetentionPolicies().keySet();
        }
        // in Influx, all measurements can exist in all retention policies,
        // (and all tickets asking for a way to know which measurements are actually
        // used in which retention policy are closed as wont-fix)
        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        for (String measurementName: client.getMeasurements().keySet()) {
            for (String retentionPolicy: retentionPolicies) {
                builder.add(new SchemaTableName(retentionPolicy, measurementName));
            }
        }
        return builder.build();
    }

    @Override
    public InfluxTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        if (client.getRetentionPolicies().containsKey(tableName.getSchemaName()) &&
            client.getMeasurements().containsKey(tableName.getTableName())) {
            return new InfluxTableHandle(tableName.getSchemaName(), tableName.getTableName());
        }
        return null;
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix) {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> result = ImmutableMap.builder();
        Collection<String> retentionPolicies = client.getRetentionPolicies().keySet();
        Collection<String> measurements = client.getMeasurements().keySet();
        Map<String, ConnectorTableMetadata> metadata = new HashMap<>();
        for (String retentionPolicy: retentionPolicies) {
            for (String measurement: measurements) {
                SchemaTableName schemaTableName = new SchemaTableName(retentionPolicy, measurement);
                if (prefix.matches(schemaTableName)) {
                    result.put(schemaTableName, metadata.computeIfAbsent(measurement,
                        k -> getTableMetadata(session, new InfluxTableHandle(schemaTableName.getSchemaName(), schemaTableName.getTableName())))
                        .getColumns());
                }
            }
        }
        return result.build();
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
        InfluxTableHandle influxTable = (InfluxTableHandle)table;
        ImmutableList.Builder<ColumnMetadata> columns = new ImmutableList.Builder<>();
        for (InfluxColumn column: client.getColumns(influxTable.getTableName())) {
            columns.add(new InfluxColumnHandle(influxTable.getSchemaName(), influxTable.getTableName(), column));
        }
        return new ConnectorTableMetadata(influxTable, columns.build());
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle) {
        ImmutableMap.Builder<String, ColumnHandle> handles = new ImmutableMap.Builder<>();
        for (ColumnMetadata column: getTableMetadata(session, tableHandle).getColumns()) {
            handles.put(column.getName(), (InfluxColumnHandle)column);
        }
        return handles.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return (InfluxColumnHandle) columnHandle;
    }

    @Override
    public boolean usesLegacyTableLayouts() {
        return false;
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        return new ConnectorTableProperties();
    }
}
