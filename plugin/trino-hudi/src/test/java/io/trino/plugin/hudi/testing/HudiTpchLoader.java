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

package io.trino.plugin.hudi.testing;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.plugin.hive.HiveStorageFormat;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.StorageFormat;
import io.trino.plugin.hive.metastore.Table;
import io.trino.spi.connector.CatalogSchemaName;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.bootstrap.index.NoOpBootstrapIndex;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.marker.MarkerType;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.intellij.lang.annotations.Language;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.trino.plugin.hive.HiveType.HIVE_STRING;
import static io.trino.plugin.hive.metastore.PrincipalPrivileges.NO_PRIVILEGES;
import static io.trino.plugin.hudi.testing.HudiTpchUtil.FIELD_UUID;
import static io.trino.plugin.hudi.testing.HudiTpchUtil.createArvoSchema;
import static io.trino.plugin.hudi.testing.HudiTpchUtil.createMetastoreColumns;
import static io.trino.plugin.hudi.testing.HudiTpchUtil.createRecordConverter;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toUnmodifiableList;
import static org.apache.hadoop.hive.metastore.TableType.EXTERNAL_TABLE;

public class HudiTpchLoader
{
    private static final Logger log = Logger.get(HudiTpchLoader.class);
    private static final List<Column> HUDI_META_COLUMNS = ImmutableList.of(
            new Column("_hoodie_commit_time", HIVE_STRING, Optional.empty()),
            new Column("_hoodie_commit_seqno", HIVE_STRING, Optional.empty()),
            new Column("_hoodie_record_key", HIVE_STRING, Optional.empty()),
            new Column("_hoodie_partition_path", HIVE_STRING, Optional.empty()),
            new Column("_hoodie_file_name", HIVE_STRING, Optional.empty()));

    private final HoodieTableType tableType;
    private final QueryRunner queryRunner;
    private final Configuration conf;
    private final String basePath;
    private final HiveMetastore metastore;
    private final CatalogSchemaName tpchCatalogSchema;
    private final CatalogSchemaName hudiCatalogSchema;

    public HudiTpchLoader(
            HoodieTableType tableType,
            QueryRunner queryRunner,
            Configuration conf,
            String basePath,
            HiveMetastore metastore,
            CatalogSchemaName tpchCatalogSchema,
            CatalogSchemaName hudiCatalogSchema)
    {
        this.tableType = requireNonNull(tableType, "tableType is null");
        this.queryRunner = requireNonNull(queryRunner, "queryRunner is null");
        this.conf = requireNonNull(conf, "conf is null");
        this.basePath = requireNonNull(basePath, "basePath is null");
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.tpchCatalogSchema = requireNonNull(tpchCatalogSchema, "tpchCatalogSchema is null");
        this.hudiCatalogSchema = requireNonNull(hudiCatalogSchema, "hudiCatalogSchema is null");
    }

    public void load(TpchTable<?> table)
    {
        HoodieJavaWriteClient<HoodieAvroPayload> writeClient = createWriteClient(table);
        RecordConverter recordConverter = createRecordConverter(table);

        @Language("SQL") String sql = generateScanSql(tpchCatalogSchema, table);
        log.info("Executing %s", sql);
        MaterializedResult result = queryRunner.execute(sql);

        List<HoodieRecord<HoodieAvroPayload>> records = result.getMaterializedRows()
                .stream()
                .map(MaterializedRow::getFields)
                .map(recordConverter::toRecord)
                .collect(Collectors.toList());
        String timestamp = "0";
        writeClient.startCommitWithTime(timestamp);
        writeClient.insert(records, timestamp);

        metastore.createTable(createMetastoreTable(table), NO_PRIVILEGES);
    }

    private String generateScanSql(CatalogSchemaName catalogSchemaName, TpchTable<?> table)
    {
        StringBuilder builder = new StringBuilder();
        builder.append("SELECT ");
        String columnList = table.getColumns()
                .stream()
                .map(c -> quote(c.getSimplifiedColumnName()))
                .collect(Collectors.joining(", "));
        builder.append(columnList);
        String tableName = String.format("%s.%s", catalogSchemaName.toString(), table.getTableName());
        builder.append(" FROM ").append(tableName);
        return builder.toString();
    }

    private Table createMetastoreTable(TpchTable<?> table)
    {
        String tablePath = "file://" + getTablePath(table);
        List<Column> columns = Stream.of(HUDI_META_COLUMNS, createMetastoreColumns(table)).
                flatMap(Collection::stream)
                .collect(toUnmodifiableList());
        // TODO: create right format
        StorageFormat storageFormat = StorageFormat.fromHiveStorageFormat(HiveStorageFormat.PARQUET);

        return Table.builder()
                .setDatabaseName(hudiCatalogSchema.getSchemaName())
                .setTableName(table.getTableName())
                .setTableType(EXTERNAL_TABLE.name())
                .setOwner(Optional.of("public"))
                .setDataColumns(columns)
                .setParameters(ImmutableMap.of("serialization.format", "1", "EXTERNAL", "TRUE"))
                .withStorage(storageBuilder -> storageBuilder
                        .setStorageFormat(storageFormat)
                        .setLocation(tablePath))
                .build();
    }

    private HoodieJavaWriteClient<HoodieAvroPayload> createWriteClient(TpchTable<?> table)
    {
        final String tableName = table.getTableName();
        final String tablePath = getTablePath(table);
        Schema schema = createArvoSchema(table);

        try {
            HoodieTableMetaClient.withPropertyBuilder()
                    .setTableType(tableType)
                    .setTableName(tableName)
                    .setBootstrapIndexClass(NoOpBootstrapIndex.class.getName())
                    .setPayloadClassName(HoodieAvroPayload.class.getName())
                    .setRecordKeyFields(FIELD_UUID)
                    .initTable(conf, tablePath);
        }
        catch (IOException e) {
            throw new RuntimeException("Could not init table " + tableName);
        }

        HoodieIndexConfig indexConfig = HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.INMEMORY).build();
        HoodieCompactionConfig compactionConfig = HoodieCompactionConfig.newBuilder().archiveCommitsWith(20, 30).build();
        HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder()
                .withPath(tablePath)
                .withSchema(schema.toString())
                .withParallelism(2, 2)
                .withDeleteParallelism(2)
                .forTable(tableName)
                .withIndexConfig(indexConfig)
                .withCompactionConfig(compactionConfig)
                .withEmbeddedTimelineServerEnabled(false)
                .withMarkersType(MarkerType.DIRECT.name())
                .build();
        return new HoodieJavaWriteClient<>(new HoodieJavaEngineContext(conf), cfg);
    }

    private String getTablePath(TpchTable<?> table)
    {
        return basePath + "/" + table.getTableName();
    }

    private static String quote(String name)
    {
        return "\"" + name + "\"";
    }
}
