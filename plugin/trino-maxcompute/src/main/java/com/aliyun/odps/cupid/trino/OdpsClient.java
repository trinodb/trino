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
package com.aliyun.odps.cupid.trino;

import com.aliyun.odps.*;
import com.aliyun.odps.cupid.CupidConf;
import com.aliyun.odps.cupid.CupidSession;
import com.aliyun.odps.task.SQLTask;
import com.aliyun.odps.utils.StringUtils;
import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.type.VarcharType;

import javax.inject.Inject;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class OdpsClient
{
    private final OdpsConfig odpsConfig;

    @Inject
    public OdpsClient(OdpsConfig config, JsonCodec<Map<String, List<OdpsTable>>> catalogCodec)
    {
        requireNonNull(config, "config is null");
        requireNonNull(catalogCodec, "catalogCodec is null");

        odpsConfig = config;

        Map<String, String> hints = new HashMap<>(2);
        hints.put("odps.sql.type.system.odps2", "true");
        hints.put("odps.sql.hive.compatible", "true");
        hints.put("odps.sql.decimal.odps2", "true");
        SQLTask.setDefaultHints(hints);

        CupidConf cupidConf = new CupidConf();
        if (System.getenv("META_LOOKUP_NAME") != null) {
            cupidConf.set("odps.project.name", System.getenv("ODPS_PROJECT_NAME"));
            cupidConf.set("odps.end.point", System.getenv("ODPS_RUNTIME_ENDPOINT"));
        } else {
            cupidConf.set("odps.project.name", config.getProject());
            cupidConf.set("odps.access.id", config.getAccessId());
            cupidConf.set("odps.access.key", config.getAccessKey());
            cupidConf.set("odps.end.point", config.getEndPoint());
            if (!StringUtils.isEmpty(config.getTunnelEndPoint())) {
                cupidConf.set("odps.tunnel.end.point", config.getTunnelEndPoint());
            }
        }
        CupidSession.setConf(cupidConf);
    }

    private Odps getOdps() {
        return CupidSession.get().odps();
    }

    public OdpsConfig getOdpsConfig() {
        return odpsConfig;
    }

    public Set<String> getProjectNames()
    {
        Set<String> projects = new HashSet<>(odpsConfig.getExtraProjectList());
        projects.add(getOdps().getDefaultProject());
        return projects;
    }

    public Set<String> getTableNames(String projectName)
    {
        requireNonNull(projectName, "projectName is null");
        Set<String> tableNames = new HashSet<>(2);
        getOdps().tables().iterable(projectName).forEach(new Consumer<Table>() {
            @Override
            public void accept(Table table) {
                tableNames.add(table.getName());
            }
        });
        return tableNames;
    }

    public OdpsTable getTable(String projectName, String tableName)
    {
        requireNonNull(projectName, "projectName is null");
        requireNonNull(tableName, "tableName is null");
        try {
            if (!getOdps().tables().exists(projectName, tableName)) {
                return null;
            }
        } catch (OdpsException e) {
            throw new TrinoException(OdpsErrorCode.ODPS_INTERNAL_ERROR, "odps getTable failed!", e);
        }
        return buildOdpsTable(getOdps().tables().get(projectName, tableName));
    }

    private OdpsTable buildOdpsTable(Table table) {
        TableSchema odpsTableSchema = table.getSchema();
        List<OdpsColumnHandle> dataColumns =
                odpsTableSchema.getColumns().stream().map(e -> OdpsUtils.buildOdpsColumn(e)).collect(Collectors.toList());
        List<OdpsColumnHandle> partitionColumns =
                odpsTableSchema.getPartitionColumns().stream().map(e -> OdpsUtils.buildOdpsColumn(e)).collect(Collectors.toList());
        return new OdpsTable(table.getName(), dataColumns, partitionColumns);
    }

    public List<OdpsPartition> getOdpsPartitions(String schemaName, String tableName, OdpsTable odpsTable, Constraint constraint) {
        Table table = getOdps().tables().get(schemaName, tableName);
        try {
            if (!table.isPartitioned()) {
                return ImmutableList.of(new OdpsPartition(new SchemaTableName(schemaName, tableName)));
            }
        } catch (OdpsException e) {
            throw new RuntimeException(e);
        }

        return table.getPartitions().stream().map(e -> new OdpsPartition(new SchemaTableName(schemaName, tableName),
                e.getPartitionSpec().toString(), getPartitionKVs(e.getPartitionSpec())))
                .map(e -> parseValuesAndFilterPartition(e, odpsTable.getPartitionColumns(), constraint))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toImmutableList());
    }

    private Map<ColumnHandle, NullableValue> getPartitionKVs(PartitionSpec odpsPartition) {
        Map<ColumnHandle, NullableValue> kvs = new LinkedHashMap<>(2);
        for (String key : odpsPartition.keys()) {
            OdpsColumnHandle columnHandle = new OdpsColumnHandle(key, VarcharType.VARCHAR, true);
            kvs.put(columnHandle, new NullableValue(VarcharType.VARCHAR, Slices.utf8Slice(odpsPartition.get(key))));
        }
        return kvs;
    }

    private Optional<OdpsPartition> parseValuesAndFilterPartition(
            OdpsPartition partition,
            List<OdpsColumnHandle> partitionColumns,
            Constraint constraint)
    {
        Map<ColumnHandle, Domain> domains = constraint.getSummary().getDomains().get();
        for (OdpsColumnHandle column : partitionColumns) {
            NullableValue value = partition.getKeys().get(column);
            Domain allowedDomain = domains.get(column);
            if (allowedDomain != null && !allowedDomain.includesNullableValue(value.getValue())) {
                return Optional.empty();
            }
        }

        if (constraint.predicate().isPresent() && !constraint.predicate().get().test(partition.getKeys())) {
            return Optional.empty();
        }

        return Optional.of(partition);
    }

    public TableSchema getTableSchema(String schemaName, String tableName) {
        return getOdps().tables().get(schemaName, tableName).getSchema();
    }

    public void dropTable(String schemaName, String tableName) {
        try {
            getOdps().tables().delete(schemaName, tableName);
        } catch (OdpsException e) {
            throw new RuntimeException(e);
        }
    }

    public void createTable(String projectName, String tableName, List<OdpsColumnHandle> inputColumns,
                            List<String> partitionedBy, boolean ignoreExisting) {
        // construct the sqlText
        StringBuilder sqlTextBuilder = new StringBuilder();
        sqlTextBuilder.append("CREATE TABLE ");
        if (ignoreExisting) {
            sqlTextBuilder.append(" IF NOT EXISTS ");
        }
        sqlTextBuilder.append(projectName).append(".`").append(tableName).append("` (");
        int i = 0;
        while (i < inputColumns.size()) {
            OdpsColumnHandle e = inputColumns.get(i);
            sqlTextBuilder.append("`").append(e.getName()).append("` ");
            sqlTextBuilder.append(OdpsUtils.toOdpsType(e.getType(), e.getIsStringType()).getTypeName());
            if (++i < inputColumns.size()) {
                sqlTextBuilder.append(",");
            } else {
                sqlTextBuilder.append(")");
            }
        }

        if (partitionedBy.size() > 0) {
            sqlTextBuilder.append(" PARTITIONED BY ");
            i = 0;
            while (i < partitionedBy.size()) {
                sqlTextBuilder.append("`").append(partitionedBy.get(i)).append("` STRING");
                if (++i < inputColumns.size()) {
                    sqlTextBuilder.append(",");
                } else {
                    sqlTextBuilder.append(")");
                }
            }
        }
        sqlTextBuilder.append(";");

        executeSql(sqlTextBuilder.toString());
    }

    private void executeSql(String sql) {
        try {
            SQLTask.run(getOdps(), sql).waitForSuccess();
        } catch (OdpsException e) {
            throw new RuntimeException(e);
        }
    }
}
