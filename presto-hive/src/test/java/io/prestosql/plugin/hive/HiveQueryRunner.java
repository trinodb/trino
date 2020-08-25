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
package io.prestosql.plugin.hive;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Module;
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.prestosql.Session;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.plugin.hive.authentication.HiveIdentity;
import io.prestosql.plugin.hive.metastore.Database;
import io.prestosql.plugin.hive.metastore.HiveMetastore;
import io.prestosql.plugin.hive.metastore.file.FileHiveMetastore;
import io.prestosql.plugin.hive.testing.TestingHivePlugin;
import io.prestosql.plugin.tpch.TpchPlugin;
import io.prestosql.spi.security.Identity;
import io.prestosql.spi.security.PrincipalType;
import io.prestosql.spi.security.SelectedRole;
import io.prestosql.testing.DistributedQueryRunner;
import io.prestosql.testing.QueryRunner;
import io.prestosql.tpch.TpchTable;
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
import static io.prestosql.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.prestosql.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.prestosql.spi.security.SelectedRole.Type.ROLE;
import static io.prestosql.testing.QueryAssertions.copyTpchTables;
import static io.prestosql.testing.TestingConnectorSession.SESSION;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
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

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
            extends DistributedQueryRunner.Builder
    {
        private Map<String, String> hiveProperties = ImmutableMap.of();
        private List<TpchTable<?>> initialTables = ImmutableList.of();
        private Function<DistributedQueryRunner, HiveMetastore> metastore = queryRunner -> {
            File baseDir = queryRunner.getCoordinator().getBaseDataDir().resolve("hive_data").toFile();
            return new FileHiveMetastore(HDFS_ENVIRONMENT, baseDir.toURI().toString(), "test", true);
        };
        private Module module = EMPTY_MODULE;

        protected Builder()
        {
            super(createSession(Optional.of(new SelectedRole(ROLE, Optional.of("admin")))));
        }

        public Builder setHiveProperties(Map<String, String> hiveProperties)
        {
            this.hiveProperties = ImmutableMap.copyOf(requireNonNull(hiveProperties, "hiveProperties is null"));
            return this;
        }

        public Builder setInitialTables(Iterable<TpchTable<?>> initialTables)
        {
            this.initialTables = ImmutableList.copyOf(requireNonNull(initialTables, "initialTables is null"));
            return this;
        }

        public Builder setMetastore(Function<DistributedQueryRunner, HiveMetastore> metastore)
        {
            this.metastore = requireNonNull(metastore, "metastore is null");
            return this;
        }

        public Builder setModule(Module module)
        {
            this.module = requireNonNull(module, "module is null");
            return this;
        }

        @Override
        public DistributedQueryRunner build()
                throws Exception
        {
            assertEquals(DateTimeZone.getDefault(), TIME_ZONE, "Timezone not configured correctly. Add -Duser.timezone=America/Bahia_Banderas to your JVM arguments");
            setupLogging();

            DistributedQueryRunner queryRunner = super.build();

            try {
                queryRunner.installPlugin(new TpchPlugin());
                queryRunner.createCatalog("tpch", "tpch");

                HiveMetastore metastore = this.metastore.apply(queryRunner);
                queryRunner.installPlugin(new TestingHivePlugin(metastore, module));

                Map<String, String> hiveProperties = ImmutableMap.<String, String>builder()
                        .put("hive.rcfile.time-zone", TIME_ZONE.getID())
                        .put("hive.parquet.time-zone", TIME_ZONE.getID())
                        .put("hive.max-partitions-per-scan", "1000")
                        .build();

                hiveProperties = new HashMap<>(hiveProperties);
                hiveProperties.putAll(this.hiveProperties);
                hiveProperties.putIfAbsent("hive.security", "sql-standard");

                Map<String, String> hiveBucketedProperties = ImmutableMap.<String, String>builder()
                        .putAll(hiveProperties)
                        .put("hive.max-initial-split-size", "10kB") // so that each bucket has multiple splits
                        .put("hive.max-split-size", "10kB") // so that each bucket has multiple splits
                        .put("hive.storage-format", "TEXTFILE") // so that there's no minimum split size for the file
                        .put("hive.compression-codec", "NONE") // so that the file is splittable
                        .build();
                queryRunner.createCatalog(HIVE_CATALOG, HIVE_CATALOG, hiveProperties);
                queryRunner.createCatalog(HIVE_BUCKETED_CATALOG, HIVE_CATALOG, hiveBucketedProperties);

                if (!initialTables.isEmpty()) {
                    populateData(queryRunner, metastore);
                }

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
                metastore.createDatabase(identity, createDatabaseMetastoreObject(TPCH_SCHEMA));
                copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, createSession(Optional.empty()), initialTables);
            }

            if (metastore.getDatabase(TPCH_BUCKETED_SCHEMA).isEmpty()) {
                metastore.createDatabase(identity, createDatabaseMetastoreObject(TPCH_BUCKETED_SCHEMA));
                copyTpchTablesBucketed(queryRunner, "tpch", TINY_SCHEMA_NAME, createBucketedSession(Optional.empty()), initialTables);
            }
        }
    }

    private static void setupLogging()
    {
        Logging logging = Logging.initialize();
        logging.setLevel("org.apache.parquet.hadoop", WARN);
    }

    private static Database createDatabaseMetastoreObject(String name)
    {
        return Database.builder()
                .setDatabaseName(name)
                .setOwnerName("public")
                .setOwnerType(PrincipalType.ROLE)
                .build();
    }

    private static Session createSession(Optional<SelectedRole> role)
    {
        return testSessionBuilder()
                .setIdentity(Identity.forUser("hive")
                        .withRoles(role.map(selectedRole -> ImmutableMap.of("hive", selectedRole))
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
                        .withRoles(role.map(selectedRole -> ImmutableMap.of("hive", selectedRole))
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
        // You need to add "--user admin" to your CLI and execute "SET ROLE admin" for queries to work
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
                .setHiveProperties(ImmutableMap.of())
                .setInitialTables(TpchTable.getTables())
                .setNodeCount(4)
                .setExtraProperties(ImmutableMap.of("http-server.http.port", "8080"))
                .setBaseDataDir(baseDataDir)
                .build();
        Thread.sleep(10);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
