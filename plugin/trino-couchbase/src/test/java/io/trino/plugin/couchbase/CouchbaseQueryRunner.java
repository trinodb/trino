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
package io.trino.plugin.couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.Scope;
import com.couchbase.client.java.json.JsonObject;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.execution.QueryIdGenerator;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingSession;
import io.trino.tpch.TpchTable;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.plugin.couchbase.CouchbaseConnectorTest.CBBUCKET;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.tpch.TpchTable.CUSTOMER;
import static io.trino.tpch.TpchTable.NATION;
import static io.trino.tpch.TpchTable.ORDERS;
import static io.trino.tpch.TpchTable.REGION;
import static java.lang.String.format;

public class CouchbaseQueryRunner
{
    private static final Logger log = Logger.get(CouchbaseQueryRunner.class);
    private static final QueryIdGenerator queryIdGenerator = new QueryIdGenerator();
    public static final String TEST_SCHEMA = "tpch";
    private static final Path SCHEMA_DIR;
    protected static final List<TpchTable<?>> TPCH_TABLES = ImmutableList.of(NATION, ORDERS, REGION, CUSTOMER, TpchTable.LINE_ITEM, TpchTable.PART, TpchTable.PART_SUPPLIER, TpchTable.SUPPLIER);

    static {
        try {
            SCHEMA_DIR = Files.createTempDirectory("cbtestschema-");
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private CouchbaseQueryRunner()
    {
    }

    public static Builder builder(CouchbaseServer server)
    {
        return new Builder(server)
                .addConnectorProperty("couchbase.cluster", server.getConnectionString())
                .addConnectorProperty("couchbase.bucket", "trino-test")
                .addConnectorProperty("couchbase.scope", "tpch")
                .addConnectorProperty("couchbase.username", server.getUsername())
                .addConnectorProperty("couchbase.password", server.getPassword())
                .addConnectorProperty("couchbase.schema-folder", SCHEMA_DIR.toString());
    }

    public static final class Builder
            extends DistributedQueryRunner.Builder<Builder>
    {
        private final Map<String, String> connectorProperties = new HashMap<>();
        private List<TpchTable<?>> initialTables = ImmutableList.of();
        private CouchbaseServer server;

        public Builder(CouchbaseServer server)
        {
            super(
                    TestingSession.testSessionBuilder()
                            .setCatalog("couchbase")
                            .setSchema(TEST_SCHEMA)
                            .build());
            this.server = server;
        }

        public Builder addConnectorProperty(String key, String value)
        {
            connectorProperties.put(key, value);
            return this;
        }

        public Builder addInitialTables(List<TpchTable<?>> initialTables)
        {
            this.initialTables = initialTables;
            return this;
        }

        @Override
        public DistributedQueryRunner build()
                throws Exception
        {
            DistributedQueryRunner queryRunner = super.build();
            try {
                queryRunner.installPlugin(new TpchPlugin());
                queryRunner.createCatalog("tpch", "tpch");

                queryRunner.installPlugin(new CouchbasePlugin());
                queryRunner.createCatalog("couchbase", "couchbase", connectorProperties);

                log.info("Loading data from %s...", TEST_SCHEMA);
                try (Cluster cluster = Cluster.connect(server.getConnectionString(), server.getUsername(), server.getPassword())) {
                    for (TpchTable<?> table : initialTables) {
                        log.info("Running import for %s", table.getTableName());
                        String tpchTableName = table.getTableName();
                        MaterializedResult rows = queryRunner.execute(format("SELECT * FROM tpch.%s.%s", TINY_SCHEMA_NAME, tpchTableName));
                        copyAndIngestTpchData(cluster, rows, server, table.getTableName(), connectorProperties);
                        generateTypeMappingFile(table.getTableName(), rows);
                        log.info("Imported %s rows for %s", rows.getRowCount(), table.getTableName());
                    }
                }
                log.info("Loading into couchbase.%s complete", TEST_SCHEMA);
                return queryRunner;
            }
            catch (Throwable e) {
                closeAllSuppress(e, queryRunner);
                throw e;
            }
        }
    }

    private static void generateTypeMappingFile(String tableName, MaterializedResult rows)
    {
        try (Writer fw = Files.newBufferedWriter(Path.of(new File(SCHEMA_DIR.toFile(), String.format("%s.%s.%s.json",
                CBBUCKET,
                TEST_SCHEMA, tableName)).toURI()))) {
            HashMap<String, Object> mappings = new HashMap<>();
            for (int i = 0; i < rows.getColumnNames().size(); i++) {
                JsonObject mapping = JsonObject.create();
                mapping.put("type", rows.getTypes().get(i).getTypeSignature().jsonValue());
                mapping.put("order", i);
                mappings.put(rows.getColumnNames().get(i), mapping);
            }
            JsonObject infer = JsonObject.from(mappings);
            JsonObject propHolder = JsonObject.create();
            propHolder.put("properties", infer);
            fw.write(propHolder.toString());
            log.info("Inferred JSON file for colume %s", propHolder);
        }
        catch (Exception ex) {
            throw new RuntimeException("Failed to generate INFER file", ex);
        }
    }

    private static void copyAndIngestTpchData(Cluster cluster, MaterializedResult rows, CouchbaseServer server, String tableName, Map<String, String> connectorProperties)
    {
        String bucketName = CBBUCKET;
        String scopeName = TEST_SCHEMA;

        Bucket bucket = cluster.bucket(bucketName);
        try {
            bucket.collections().createScope(scopeName);
        }
        catch (Exception _) {
            // noop
        }
        Scope scope = bucket.scope(scopeName);

        bucket.collections().createCollection(scopeName, tableName);

        Collection target = scope.collection(tableName);

        List<String> columns = rows.getColumnNames();
        List<Type> types = rows.getTypes();
        for (MaterializedRow row : rows) {
            JsonObject document = JsonObject.create();
            for (int i = 0; i < columns.size(); i++) {
                String columnName = columns.get(i);
                document.put(columnName, convertType(row.getField(i), types.get(i)));
            }
            target.upsert(UUID.randomUUID().toString(), document);
        }
        // let the dust settle
        try {
            Thread.sleep(1000L);
        }
        catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static Object convertType(Object value, Type type)
    {
        if (value == null) {
            return null;
        }

        if (type == BOOLEAN
                || type instanceof VarcharType
                || type == IntegerType.INTEGER
                || type == BigintType.BIGINT
                || type == DoubleType.DOUBLE) {
            return value;
        }
        else if (type == DATE) {
            return ChronoUnit.DAYS.between(LocalDate.ofEpochDay(0), (LocalDate) value);
        }
        else {
            throw new RuntimeException(String.format("Unsupported type: %s -- class: %s", type, value.getClass()));
        }
    }

    static void main()
            throws Exception
    {
        Logging.initialize();
        QueryRunner queryRunner = builder(new CouchbaseServer("trino-test"))
                .addInitialTables(TPCH_TABLES)
                .addCoordinatorProperty("http-server.http.port", "8080")
                .setExtraProperties(ImmutableMap.<String, String>builder()
                        .put("sql.default-catalog", "tpch")
                        .put("sql.default-schema", "tiny")
                        .buildOrThrow())
                .withProtocolSpooling("json+zstd")
                .build();
        Logger log = Logger.get(CouchbaseQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
