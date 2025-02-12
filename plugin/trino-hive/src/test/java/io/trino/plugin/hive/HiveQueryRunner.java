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
package io.trino.plugin.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.Session;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metastore.Database;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.plugin.tpcds.TpcdsPlugin;
import io.trino.plugin.tpch.ColumnNaming;
import io.trino.plugin.tpch.DecimalTypeMapping;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.security.Identity;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.security.SelectedRole;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.intellij.lang.annotations.Language;
import org.joda.time.DateTimeZone;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static io.airlift.log.Level.WARN;
import static io.airlift.units.Duration.nanosSince;
import static io.trino.plugin.hive.TestingHiveUtils.getConnectorService;
import static io.trino.plugin.tpch.ColumnNaming.SIMPLIFIED;
import static io.trino.plugin.tpch.DecimalTypeMapping.DOUBLE;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.spi.security.SelectedRole.Type.ROLE;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.nio.file.Files.createDirectories;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public final class HiveQueryRunner
{
    static {
        Logging logging = Logging.initialize();
        logging.setLevel("org.apache.parquet.hadoop", WARN);
    }

    private static final Logger log = Logger.get(HiveQueryRunner.class);

    private HiveQueryRunner() {}

    public static final String HIVE_CATALOG = "hive";
    private static final String HIVE_BUCKETED_CATALOG = "hive_bucketed";
    public static final String TPCH_SCHEMA = "tpch";
    private static final String TPCH_BUCKETED_SCHEMA = "tpch_bucketed";
    private static final DateTimeZone TIME_ZONE = DateTimeZone.forID("America/Bahia_Banderas");

    public static QueryRunner create()
            throws Exception
    {
        return builder().build();
    }

    public static Builder<Builder<?>> builder()
    {
        return new Builder<>();
    }

    public static Builder<Builder<?>> builder(Session defaultSession)
    {
        return new Builder<>(defaultSession);
    }

    public static class Builder<SELF extends Builder<?>>
            extends DistributedQueryRunner.Builder<SELF>
    {
        private boolean skipTimezoneSetup;
        private ImmutableMap.Builder<String, String> hiveProperties = ImmutableMap.builder();
        private List<TpchTable<?>> initialTables = ImmutableList.of();
        private Optional<String> initialSchemasLocationBase = Optional.empty();
        private Optional<Function<DistributedQueryRunner, HiveMetastore>> metastore = Optional.empty();
        private boolean tpcdsCatalogEnabled;
        private boolean tpchBucketedCatalogEnabled;
        private boolean createTpchSchemas = true;
        private ColumnNaming tpchColumnNaming = SIMPLIFIED;
        private DecimalTypeMapping tpchDecimalTypeMapping = DOUBLE;

        protected Builder()
        {
            this(createSession(Optional.of(new SelectedRole(ROLE, Optional.of("admin")))));
        }

        protected Builder(Session defaultSession)
        {
            super(defaultSession);
        }

        @CanIgnoreReturnValue
        public SELF setSkipTimezoneSetup(boolean skipTimezoneSetup)
        {
            this.skipTimezoneSetup = skipTimezoneSetup;
            return self();
        }

        @CanIgnoreReturnValue
        public SELF setHiveProperties(Map<String, String> hiveProperties)
        {
            this.hiveProperties = ImmutableMap.<String, String>builder()
                    .putAll(requireNonNull(hiveProperties, "hiveProperties is null"));
            return self();
        }

        @CanIgnoreReturnValue
        public SELF addHiveProperty(String key, String value)
        {
            this.hiveProperties.put(key, value);
            return self();
        }

        @CanIgnoreReturnValue
        public SELF setInitialTables(Iterable<TpchTable<?>> initialTables)
        {
            this.initialTables = ImmutableList.copyOf(requireNonNull(initialTables, "initialTables is null"));
            return self();
        }

        @CanIgnoreReturnValue
        public SELF setInitialSchemasLocationBase(String initialSchemasLocationBase)
        {
            this.initialSchemasLocationBase = Optional.of(initialSchemasLocationBase);
            return self();
        }

        @CanIgnoreReturnValue
        public SELF setMetastore(Function<DistributedQueryRunner, HiveMetastore> metastore)
        {
            this.metastore = Optional.of(metastore);
            return self();
        }

        @CanIgnoreReturnValue
        public SELF setTpcdsCatalogEnabled(boolean tpcdsCatalogEnabled)
        {
            this.tpcdsCatalogEnabled = tpcdsCatalogEnabled;
            return self();
        }

        @CanIgnoreReturnValue
        public SELF setTpchBucketedCatalogEnabled(boolean tpchBucketedCatalogEnabled)
        {
            this.tpchBucketedCatalogEnabled = tpchBucketedCatalogEnabled;
            return self();
        }

        @CanIgnoreReturnValue
        public SELF setCreateTpchSchemas(boolean createTpchSchemas)
        {
            this.createTpchSchemas = createTpchSchemas;
            return self();
        }

        @SuppressWarnings("unused")
        @CanIgnoreReturnValue
        public SELF setTpchColumnNaming(ColumnNaming tpchColumnNaming)
        {
            this.tpchColumnNaming = requireNonNull(tpchColumnNaming, "tpchColumnNaming is null");
            return self();
        }

        @SuppressWarnings("unused")
        @CanIgnoreReturnValue
        public SELF setTpchDecimalTypeMapping(DecimalTypeMapping tpchDecimalTypeMapping)
        {
            this.tpchDecimalTypeMapping = requireNonNull(tpchDecimalTypeMapping, "tpchDecimalTypeMapping is null");
            return self();
        }

        @Override
        public DistributedQueryRunner build()
                throws Exception
        {
            DistributedQueryRunner queryRunner = super.build();

            try {
                queryRunner.installPlugin(new TpchPlugin());
                Map<String, String> tpchCatalogProperties = ImmutableMap.<String, String>builder()
                        .put("tpch.column-naming", tpchColumnNaming.name())
                        .put("tpch.double-type-mapping", tpchDecimalTypeMapping.name())
                        .buildOrThrow();
                queryRunner.createCatalog("tpch", "tpch", tpchCatalogProperties);

                if (tpcdsCatalogEnabled) {
                    queryRunner.installPlugin(new TpcdsPlugin());
                    queryRunner.createCatalog("tpcds", "tpcds");
                }

                Optional<HiveMetastore> metastore = this.metastore.map(factory -> factory.apply(queryRunner));
                Path dataDir = queryRunner.getCoordinator().getBaseDataDir().resolve("hive_data");

                if (metastore.isEmpty() && !hiveProperties.buildOrThrow().containsKey("hive.metastore")) {
                    hiveProperties.put("hive.metastore", "file");
                    hiveProperties.put("hive.metastore.catalog.dir", queryRunner.getCoordinator().getBaseDataDir().resolve("hive_data").toString());
                }
                if (!hiveProperties.buildOrThrow().containsKey("fs.hadoop.enabled")) {
                    hiveProperties.put("fs.hadoop.enabled", "true");
                }

                queryRunner.installPlugin(new TestingHivePlugin(dataDir, metastore));

                Map<String, String> hiveProperties = new HashMap<>();
                if (!skipTimezoneSetup) {
                    assertThat(DateTimeZone.getDefault())
                            .describedAs("Timezone not configured correctly. Add -Duser.timezone=America/Bahia_Banderas to your JVM arguments")
                            .isEqualTo(TIME_ZONE);
                    hiveProperties.put("hive.rcfile.time-zone", TIME_ZONE.getID());
                    hiveProperties.put("hive.parquet.time-zone", TIME_ZONE.getID());
                }
                hiveProperties.put("hive.max-partitions-per-scan", "1000");
                hiveProperties.put("hive.max-partitions-for-eager-load", "1000");
                hiveProperties.put("hive.security", "sql-standard");
                hiveProperties.putAll(this.hiveProperties.buildOrThrow());

                if (tpchBucketedCatalogEnabled) {
                    Map<String, String> hiveBucketedProperties = ImmutableMap.<String, String>builder()
                            .putAll(hiveProperties)
                            .put("hive.max-initial-split-size", "10kB") // so that each bucket has multiple splits
                            .put("hive.max-split-size", "10kB") // so that each bucket has multiple splits
                            .put("hive.storage-format", "TEXTFILE") // so that there's no minimum split size for the file
                            .buildOrThrow();
                    hiveBucketedProperties = new HashMap<>(hiveBucketedProperties);
                    hiveBucketedProperties.put("hive.compression-codec", "NONE"); // so that the file is splittable
                    queryRunner.createCatalog(HIVE_BUCKETED_CATALOG, "hive", hiveBucketedProperties);
                }

                queryRunner.createCatalog(HIVE_CATALOG, "hive", hiveProperties);

                if (createTpchSchemas) {
                    populateData(queryRunner);
                }

                return queryRunner;
            }
            catch (Exception e) {
                queryRunner.close();
                throw e;
            }
        }

        private void populateData(QueryRunner queryRunner)
        {
            HiveMetastore metastore = getConnectorService(queryRunner, HiveMetastoreFactory.class)
                    .createMetastore(Optional.empty());
            if (metastore.getDatabase(TPCH_SCHEMA).isEmpty()) {
                metastore.createDatabase(createDatabaseMetastoreObject(TPCH_SCHEMA, initialSchemasLocationBase));
                copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, initialTables);
            }

            if (tpchBucketedCatalogEnabled && metastore.getDatabase(TPCH_BUCKETED_SCHEMA).isEmpty()) {
                metastore.createDatabase(createDatabaseMetastoreObject(TPCH_BUCKETED_SCHEMA, initialSchemasLocationBase));
                Session session = createBucketedSession(Optional.empty());
                copyTpchTablesBucketed(queryRunner, "tpch", TINY_SCHEMA_NAME, session, initialTables, tpchColumnNaming);
            }
        }
    }

    private static Database createDatabaseMetastoreObject(String name, Optional<String> locationBase)
    {
        return Database.builder()
                .setLocation(locationBase.map(base -> base + "/" + name))
                .setDatabaseName(name)
                .setOwnerName(Optional.of("public"))
                .setOwnerType(Optional.of(PrincipalType.ROLE))
                .build();
    }

    private static Session createSession(Optional<SelectedRole> role)
    {
        return testSessionBuilder()
                .setIdentity(Identity.forUser("hive")
                        .withConnectorRoles(role.map(selectedRole -> ImmutableMap.of(HIVE_CATALOG, selectedRole))
                                .orElse(ImmutableMap.of()))
                        .build())
                .setCatalog(HIVE_CATALOG)
                .setSchema(TPCH_SCHEMA)
                .build();
    }

    public static Session createBucketedSession(Optional<SelectedRole> role)
    {
        return testSessionBuilder()
                .setIdentity(Identity.forUser("hive")
                        .withConnectorRoles(role.map(selectedRole -> ImmutableMap.of(
                                        HIVE_CATALOG, selectedRole,
                                        HIVE_BUCKETED_CATALOG, selectedRole))
                                .orElse(ImmutableMap.of()))
                        .build())
                .setCatalog(HIVE_BUCKETED_CATALOG)
                .setSchema(TPCH_BUCKETED_SCHEMA)
                .build();
    }

    private static void copyTpchTablesBucketed(
            QueryRunner queryRunner,
            String sourceCatalog,
            String sourceSchema,
            Session session,
            Iterable<TpchTable<?>> tables,
            ColumnNaming columnNaming)
    {
        for (TpchTable<?> table : tables) {
            copyTableBucketed(queryRunner, new QualifiedObjectName(sourceCatalog, sourceSchema, table.getTableName().toLowerCase(ENGLISH)), table, session, columnNaming);
        }
    }

    private static void copyTableBucketed(QueryRunner queryRunner, QualifiedObjectName tableName, TpchTable<?> table, Session session, ColumnNaming columnNaming)
    {
        long start = System.nanoTime();
        @Language("SQL") String sql;
        switch (tableName.objectName()) {
            case "part":
            case "partsupp":
            case "supplier":
            case "nation":
            case "region":
                sql = format("CREATE TABLE %s AS SELECT * FROM %s", tableName.objectName(), tableName);
                break;
            case "lineitem":
                sql = format(
                        "CREATE TABLE %s WITH (bucketed_by=array['%s'], bucket_count=11) AS SELECT * FROM %s",
                        tableName.objectName(),
                        columnNaming.getName(table.getColumn("orderkey")),
                        tableName);
                break;
            case "customer":
            case "orders":
                sql = format(
                        "CREATE TABLE %s WITH (bucketed_by=array['%s'], bucket_count=11) AS SELECT * FROM %s",
                        tableName.objectName(),
                        columnNaming.getName(table.getColumn("custkey")),
                        tableName);
                break;
            default:
                throw new UnsupportedOperationException();
        }
        long rows = (Long) queryRunner.execute(session, sql).getMaterializedRows().get(0).getField(0);
        log.info("Imported %s rows from %s in %s", rows, tableName, nanosSince(start));
    }

    public static final class DefaultHiveQueryRunnerMain
    {
        public static void main(String[] args)
                throws Exception
        {
            Optional<Path> baseDataDir = Optional.empty();
            if (args.length > 0) {
                if (args.length != 1) {
                    System.err.println("usage: HiveQueryRunner [baseDataDir]");
                    System.exit(1);
                }

                Path path = Paths.get(args[0]);
                createDirectories(path);
                baseDataDir = Optional.of(path);
            }

            QueryRunner queryRunner = builder()
                    .addCoordinatorProperty("http-server.http.port", "8080")
                    .setHiveProperties(ImmutableMap.of("hive.security", "allow-all"))
                    .setSkipTimezoneSetup(true)
                    .setInitialTables(TpchTable.getTables())
                    .setBaseDataDir(baseDataDir)
                    .setTpcdsCatalogEnabled(true)
                    // Uncomment to enable standard column naming (column names to be prefixed with the first letter of the table name, e.g.: o_orderkey vs orderkey)
                    // and standard column types (decimals vs double for some columns). This will allow running unmodified tpch queries on the cluster.
                    //.setTpchColumnNaming(ColumnNaming.STANDARD)
                    //.setTpchDecimalTypeMapping(DecimalTypeMapping.DECIMAL)
                    .build();
            log.info("======== SERVER STARTED ========");
            log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
        }
    }

    public static final class HiveGlueQueryRunnerMain
    {
        private HiveGlueQueryRunnerMain() {}

        public static void main(String[] args)
                throws Exception
        {
            // Requires AWS credentials, which can be provided any way supported by the DefaultProviderChain
            // See https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html#credentials-default
            //noinspection resource
            DistributedQueryRunner queryRunner = HiveQueryRunner.builder(testSessionBuilder()
                            .setCatalog("hive")
                            .setSchema("tpch")
                            .build())
                    .addCoordinatorProperty("http-server.http.port", "8080")
                    .addHiveProperty("hive.metastore", "glue")
                    .addHiveProperty("hive.metastore.glue.default-warehouse-dir", "local:///glue")
                    .addHiveProperty("hive.security", "allow-all")
                    .setCreateTpchSchemas(false)
                    .build();

            Logger log = Logger.get(HiveGlueQueryRunnerMain.class);
            log.info("======== SERVER STARTED ========");
            log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
        }
    }
}
