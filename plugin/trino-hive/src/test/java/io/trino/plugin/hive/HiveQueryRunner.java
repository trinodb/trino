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
import com.google.inject.Module;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.trino.Session;
import io.trino.metadata.QualifiedObjectName;
import io.trino.plugin.exchange.FileSystemExchangePlugin;
import io.trino.plugin.hive.authentication.HiveIdentity;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.MetastoreConfig;
import io.trino.plugin.hive.metastore.file.FileHiveMetastore;
import io.trino.plugin.hive.metastore.file.FileHiveMetastoreConfig;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.security.Identity;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.security.SelectedRole;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.tpch.TpchTable;
import org.intellij.lang.annotations.Language;
import org.joda.time.DateTimeZone;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static com.google.inject.util.Modules.EMPTY_MODULE;
import static io.airlift.log.Level.WARN;
import static io.airlift.units.Duration.nanosSince;
import static io.trino.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.trino.plugin.hive.security.HiveSecurityModule.SQL_STANDARD;
import static io.trino.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.trino.spi.security.SelectedRole.Type.ROLE;
import static io.trino.testing.QueryAssertions.copyTpchTables;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.nio.file.Files.createDirectories;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;

public final class HiveQueryRunner
{
    private static final Logger log = Logger.get(HiveQueryRunner.class);

    private HiveQueryRunner() {}

    public static final String HIVE_CATALOG = "hive";
    private static final String HIVE_BUCKETED_CATALOG = "hive_bucketed";
    public static final String TPCH_SCHEMA = "tpch";
    private static final String TPCH_BUCKETED_SCHEMA = "tpch_bucketed";
    private static final DateTimeZone TIME_ZONE = DateTimeZone.forID("America/Bahia_Banderas");

    public static DistributedQueryRunner create()
            throws Exception
    {
        return builder().build();
    }

    public static Builder<Builder<?>> builder()
    {
        return new Builder<>();
    }

    public static class Builder<SELF extends Builder<?>>
            extends DistributedQueryRunner.Builder<SELF>
    {
        private boolean skipTimezoneSetup;
        private ImmutableMap.Builder<String, String> hiveProperties = ImmutableMap.builder();
        private Map<String, String> exchangeManagerProperties = ImmutableMap.of();
        private List<TpchTable<?>> initialTables = ImmutableList.of();
        private Optional<String> initialSchemasLocationBase = Optional.empty();
        private Function<DistributedQueryRunner, HiveMetastore> metastore = queryRunner -> {
            File baseDir = queryRunner.getCoordinator().getBaseDataDir().resolve("hive_data").toFile();
            return new FileHiveMetastore(
                    new NodeVersion("test_version"),
                    HDFS_ENVIRONMENT,
                    new MetastoreConfig(),
                    new FileHiveMetastoreConfig()
                            .setCatalogDirectory(baseDir.toURI().toString())
                            .setMetastoreUser("test"));
        };
        private Module module = EMPTY_MODULE;

        protected Builder()
        {
            super(createSession(Optional.of(new SelectedRole(ROLE, Optional.of("admin")))));
        }

        public SELF setSkipTimezoneSetup(boolean skipTimezoneSetup)
        {
            this.skipTimezoneSetup = skipTimezoneSetup;
            return self();
        }

        public SELF setHiveProperties(Map<String, String> hiveProperties)
        {
            this.hiveProperties = ImmutableMap.<String, String>builder()
                    .putAll(requireNonNull(hiveProperties, "hiveProperties is null"));
            return self();
        }

        public SELF addHiveProperty(String key, String value)
        {
            this.hiveProperties.put(key, value);
            return self();
        }

        public SELF setExchangeManagerProperties(Map<String, String> exchangeManagerProperties)
        {
            this.exchangeManagerProperties = ImmutableMap.copyOf(requireNonNull(exchangeManagerProperties, "exchangeManagerProperties is null"));
            return self();
        }

        public SELF setInitialTables(Iterable<TpchTable<?>> initialTables)
        {
            this.initialTables = ImmutableList.copyOf(requireNonNull(initialTables, "initialTables is null"));
            return self();
        }

        public SELF setInitialSchemasLocationBase(String initialSchemasLocationBase)
        {
            this.initialSchemasLocationBase = Optional.of(initialSchemasLocationBase);
            return self();
        }

        public SELF setMetastore(Function<DistributedQueryRunner, HiveMetastore> metastore)
        {
            this.metastore = requireNonNull(metastore, "metastore is null");
            return self();
        }

        public SELF setModule(Module module)
        {
            this.module = requireNonNull(module, "module is null");
            return self();
        }

        @Override
        public DistributedQueryRunner build()
                throws Exception
        {
            setupLogging();

            DistributedQueryRunner queryRunner = super.build();

            try {
                queryRunner.installPlugin(new TpchPlugin());
                queryRunner.createCatalog("tpch", "tpch");

                HiveMetastore metastore = this.metastore.apply(queryRunner);
                queryRunner.installPlugin(new TestingHivePlugin(metastore, module));

                if (!exchangeManagerProperties.isEmpty()) {
                    queryRunner.installPlugin(new FileSystemExchangePlugin());
                    queryRunner.loadExchangeManager("filesystem", exchangeManagerProperties);
                }

                Map<String, String> hiveProperties = new HashMap<>();
                if (!skipTimezoneSetup) {
                    assertEquals(DateTimeZone.getDefault(), TIME_ZONE, "Timezone not configured correctly. Add -Duser.timezone=America/Bahia_Banderas to your JVM arguments");
                    hiveProperties.put("hive.rcfile.time-zone", TIME_ZONE.getID());
                    hiveProperties.put("hive.parquet.time-zone", TIME_ZONE.getID());
                }
                hiveProperties.put("hive.max-partitions-per-scan", "1000");
                hiveProperties.put("hive.security", SQL_STANDARD);
                hiveProperties.putAll(this.hiveProperties.build());

                Map<String, String> hiveBucketedProperties = ImmutableMap.<String, String>builder()
                        .putAll(hiveProperties)
                        .put("hive.max-initial-split-size", "10kB") // so that each bucket has multiple splits
                        .put("hive.max-split-size", "10kB") // so that each bucket has multiple splits
                        .put("hive.storage-format", "TEXTFILE") // so that there's no minimum split size for the file
                        .put("hive.compression-codec", "NONE") // so that the file is splittable
                        .build();
                queryRunner.createCatalog(HIVE_CATALOG, "hive", hiveProperties);
                queryRunner.createCatalog(HIVE_BUCKETED_CATALOG, "hive", hiveBucketedProperties);

                populateData(queryRunner, metastore);

                return queryRunner;
            }
            catch (Exception e) {
                queryRunner.close();
                throw e;
            }
        }

        private void populateData(DistributedQueryRunner queryRunner, HiveMetastore metastore)
        {
            HiveIdentity identity = new HiveIdentity(SESSION);
            if (metastore.getDatabase(TPCH_SCHEMA).isEmpty()) {
                metastore.createDatabase(identity, createDatabaseMetastoreObject(TPCH_SCHEMA, initialSchemasLocationBase));
                copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, createSession(Optional.empty()), initialTables);
            }

            if (metastore.getDatabase(TPCH_BUCKETED_SCHEMA).isEmpty()) {
                metastore.createDatabase(identity, createDatabaseMetastoreObject(TPCH_BUCKETED_SCHEMA, initialSchemasLocationBase));
                copyTpchTablesBucketed(queryRunner, "tpch", TINY_SCHEMA_NAME, createBucketedSession(Optional.empty()), initialTables);
            }
        }
    }

    private static void setupLogging()
    {
        Logging logging = Logging.initialize();
        logging.setLevel("org.apache.parquet.hadoop", WARN);
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
                        .withConnectorRoles(role.map(selectedRole -> ImmutableMap.of(
                                HIVE_CATALOG, selectedRole,
                                HIVE_BUCKETED_CATALOG, selectedRole))
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
            Iterable<TpchTable<?>> tables)
    {
        log.info("Loading data from %s.%s...", sourceCatalog, sourceSchema);
        long startTime = System.nanoTime();
        for (TpchTable<?> table : tables) {
            copyTableBucketed(queryRunner, new QualifiedObjectName(sourceCatalog, sourceSchema, table.getTableName().toLowerCase(ENGLISH)), session);
        }
        log.info("Loading from %s.%s complete in %s", sourceCatalog, sourceSchema, nanosSince(startTime).toString(SECONDS));
    }

    private static void copyTableBucketed(QueryRunner queryRunner, QualifiedObjectName table, Session session)
    {
        long start = System.nanoTime();
        log.info("Running import for %s", table.getObjectName());
        @Language("SQL") String sql;
        switch (table.getObjectName()) {
            case "part":
            case "partsupp":
            case "supplier":
            case "nation":
            case "region":
                sql = format("CREATE TABLE %s AS SELECT * FROM %s", table.getObjectName(), table);
                break;
            case "lineitem":
                sql = format("CREATE TABLE %s WITH (bucketed_by=array['orderkey'], bucket_count=11) AS SELECT * FROM %s", table.getObjectName(), table);
                break;
            case "customer":
                sql = format("CREATE TABLE %s WITH (bucketed_by=array['custkey'], bucket_count=11) AS SELECT * FROM %s", table.getObjectName(), table);
                break;
            case "orders":
                sql = format("CREATE TABLE %s WITH (bucketed_by=array['custkey'], bucket_count=11) AS SELECT * FROM %s", table.getObjectName(), table);
                break;
            default:
                throw new UnsupportedOperationException();
        }
        long rows = (Long) queryRunner.execute(session, sql).getMaterializedRows().get(0).getField(0);
        log.info("Imported %s rows for %s in %s", rows, table.getObjectName(), nanosSince(start).convertToMostSuccinctTimeUnit());
    }

    public static void main(String[] args)
            throws Exception
    {
        // You need to add "--user admin" to your CLI and execute "SET ROLE admin IN hive" for queries to work
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

        DistributedQueryRunner queryRunner = HiveQueryRunner.builder()
                .setExtraProperties(ImmutableMap.of("http-server.http.port", "8080"))
                .setSkipTimezoneSetup(true)
                .setHiveProperties(ImmutableMap.of())
                .setInitialTables(TpchTable.getTables())
                .setBaseDataDir(baseDataDir)
                .build();
        Thread.sleep(10);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
