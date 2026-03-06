package io.trino.plugin.couchbase;

import com.couchbase.client.core.api.query.CoreQueryResult;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.Scope;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.QueryResult;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.client.ClientCapabilities;
import io.trino.execution.QueryIdGenerator;
import io.trino.metadata.SessionPropertyManager;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.security.Identity;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.tpch.TpchTable;
import io.trino.spi.type.Type;
import org.json.simple.JSONObject;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.*;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.testing.Closeables.closeAllSuppress;
import static io.trino.plugin.couchbase.TestCouchbaseConnector.CBBUCKET;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.testing.TestingSession.DEFAULT_TIME_ZONE_KEY;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;

public class CouchbaseQueryRunner {
    private static final Logger log = Logger.get(CouchbaseQueryRunner.class);
    private static final QueryIdGenerator queryIdGenerator = new QueryIdGenerator();
    public static final String TEST_SCHEMA = "tpch";
    private static final Path SCHEMA_DIR;

    static {
        try {
            SCHEMA_DIR = Files.createTempDirectory("cbtestschema-");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Builder builder(CouchbaseServer server) {

        return new Builder(server)
                .addConnectorProperty("couchbase.cluster", server.getConnectionString())
                .addConnectorProperty("couchbase.bucket", "trino-test")
                .addConnectorProperty("couchbase.username", server.getUsername())
                .addConnectorProperty("couchbase.password", server.getPassword())
                .addConnectorProperty("couchbase.schema-folder", SCHEMA_DIR.toString());
    }

    private static Session.SessionBuilder testSessionBuilder() {
        return testSessionBuilder(new SessionPropertyManager());
    }

    private static Session.SessionBuilder testSessionBuilder(SessionPropertyManager sessionPropertyManager) {
        return Session.builder(sessionPropertyManager)
                .setQueryId(queryIdGenerator.createNextQueryId())
                .setIdentity(Identity.ofUser("user"))
                .setOriginalIdentity(Identity.ofUser("user"))
                .setSource("test")
                .setCatalog("catalog")
                .setSchema("schema")
                .setTimeZoneKey(DEFAULT_TIME_ZONE_KEY)
                .setLocale(ENGLISH)
                .setClientCapabilities(Arrays.stream(ClientCapabilities.values()).map(Enum::name)
                        .collect(toImmutableSet()))
                .setRemoteUserAddress("address")
                .setUserAgent("agent");
    }

    public static final class Builder
            extends DistributedQueryRunner.Builder<Builder>
    {
        private final Map<String, String> connectorProperties = new HashMap<>();
        private List<TpchTable<?>> initialTables = ImmutableList.of();
        private CouchbaseServer server;

        public Builder(CouchbaseServer server) {
            super(
                    testSessionBuilder()
                            .setCatalog("couchbase")
                            .setSchema(TEST_SCHEMA)
                            .build()
            );
            this.server = server;
        }

        public Builder addConnectorProperty(String key, String value) {
            connectorProperties.put(key, value);
            return this;
        }

        public Builder addInitialTables(List<TpchTable<?>> initialTables) {
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
                        generateInferFiles(cluster, table.getTableName());
                        log.info("Imported %s rows for %s", rows.getRowCount(), table.getTableName());
                    }
                }
                log.info("Loading from couchbase.%s complete", TEST_SCHEMA);
                return queryRunner;
            }
            catch (Throwable e) {
                closeAllSuppress(e, queryRunner);
                throw e;
            }
        }
    }

    private static void generateInferFiles(Cluster cluster, String tableName) {
        QueryResult qr = cluster.query(String.format("INFER `%s`.`%s`.`%s`", CBBUCKET, TEST_SCHEMA, tableName));
        try {
            qr.rowsAsObject();
            throw new IllegalStateException("Infer didn't throw an Exception, this indicates that a bug in the SDK has been fixed");
        } catch (Exception ex) {
            if (ex.getMessage().startsWith("Deserialization of content into target class com.couchbase.client.java.json.JsonObject failed")) {
                try {
                    Field field = QueryResult.class.getDeclaredField("internal");
                    field.setAccessible(true);
                    CoreQueryResult internal = (CoreQueryResult) field.get(qr);
                    List<JsonObject> objList = new ArrayList<>();
                    internal.rows().forEach(e -> {
                                JsonObject obj = JsonObject.create();
                                obj.put("content", JsonArray.fromJson(e.data()));
                                objList.add(obj);
                            }
                    );
                    JsonObject r = objList.get(0);
                    try (FileWriter fw = new FileWriter(new File(SCHEMA_DIR.toFile(), String.format("%s.%s.%s.json", CBBUCKET, TEST_SCHEMA, tableName)))) {
                        fw.write(r.toString());
                    } catch (IOException e) {
                        throw new RuntimeException("Failed to generate INFER file", e);
                    }
                } catch (NoSuchFieldException | IllegalAccessException e) {
                    throw new RuntimeException("Failed to obtain INFER result", e);
                }
            } else {
                throw ex;
            }
        }
    }

    private static void copyAndIngestTpchData(Cluster cluster, MaterializedResult rows, CouchbaseServer server, String tableName, Map<String, String> connectorProperties)
    {
        String bucketName = CBBUCKET;
        String scopeName = TEST_SCHEMA;

        Bucket bucket = cluster.bucket(bucketName);
        try {
            bucket.collections().createScope(scopeName);
        } catch (Exception _) {
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
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static Object convertType(Object value, Type type) {
        if (type == BOOLEAN) {
            return value;
        } else if (type == DATE) {
            return ((LocalDate) value).toString();
        }
        return value;
    }
}
