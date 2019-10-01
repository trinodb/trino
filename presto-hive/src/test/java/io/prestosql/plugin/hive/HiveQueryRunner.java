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
import io.airlift.log.Logger;
import io.airlift.log.Logging;
import io.airlift.tpch.TpchTable;
import io.prestosql.Session;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.plugin.hive.authentication.HiveIdentity;
import io.prestosql.plugin.hive.metastore.Database;
import io.prestosql.plugin.hive.metastore.file.FileHiveMetastore;
import io.prestosql.plugin.tpch.TpchPlugin;
import io.prestosql.spi.security.Identity;
import io.prestosql.spi.security.PrincipalType;
import io.prestosql.spi.security.SelectedRole;
import io.prestosql.testing.QueryRunner;
import io.prestosql.tests.DistributedQueryRunner;
import org.intellij.lang.annotations.Language;
import org.joda.time.DateTimeZone;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;

import static io.airlift.log.Level.WARN;
import static io.airlift.units.Duration.nanosSince;
import static io.prestosql.plugin.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static io.prestosql.plugin.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.prestosql.spi.security.SelectedRole.Type.ROLE;
import static io.prestosql.testing.TestingConnectorSession.SESSION;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static io.prestosql.tests.QueryAssertions.copyTpchTables;
import static java.lang.String.format;
import static java.nio.file.Files.createDirectories;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;

public final class HiveQueryRunner
{
    private static final Logger log = Logger.get(HiveQueryRunner.class);

    private HiveQueryRunner()
    {
    }

    public static final String HIVE_CATALOG = "hive";
    public static final String HIVE_BUCKETED_CATALOG = "hive_bucketed";
    public static final String TPCH_SCHEMA = "tpch";
    private static final String TPCH_BUCKETED_SCHEMA = "tpch_bucketed";
    private static final DateTimeZone TIME_ZONE = DateTimeZone.forID("America/Bahia_Banderas");

    public static DistributedQueryRunner createQueryRunner(TpchTable<?>... tables)
            throws Exception
    {
        return createQueryRunner(ImmutableList.copyOf(tables));
    }

    public static DistributedQueryRunner createQueryRunner(Iterable<TpchTable<?>> tables)
            throws Exception
    {
        return createQueryRunner(tables, ImmutableMap.of(), Optional.empty());
    }

    public static DistributedQueryRunner createQueryRunner(Iterable<TpchTable<?>> tables, Map<String, String> extraProperties, Optional<Path> baseDataDir)
            throws Exception
    {
        return createQueryRunner(tables, extraProperties, "sql-standard", ImmutableMap.of(), baseDataDir);
    }

    public static DistributedQueryRunner createQueryRunner(Iterable<TpchTable<?>> tables, Map<String, String> extraProperties, String security, Map<String, String> extraHiveProperties, Optional<Path> baseDataDir)
            throws Exception
    {
        assertEquals(DateTimeZone.getDefault(), TIME_ZONE, "Timezone not configured correctly. Add -Duser.timezone=America/Bahia_Banderas to your JVM arguments");
        setupLogging();

        DistributedQueryRunner queryRunner = DistributedQueryRunner
                .builder(createSession(Optional.of(new SelectedRole(ROLE, Optional.of("admin")))))
                .setNodeCount(4)
                .setExtraProperties(extraProperties)
                .setBaseDataDir(baseDataDir)
                .build();

        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.createCatalog("tpch", "tpch");

            File baseDir = queryRunner.getCoordinator().getBaseDataDir().resolve("hive_data").toFile();

            FileHiveMetastore metastore = new FileHiveMetastore(HDFS_ENVIRONMENT, baseDir.toURI().toString(), "test");
            queryRunner.installPlugin(new HivePlugin(HIVE_CATALOG, Optional.of(metastore)));

            Map<String, String> hiveProperties = ImmutableMap.<String, String>builder()
                    .putAll(extraHiveProperties)
                    .put("hive.time-zone", TIME_ZONE.getID())
                    .put("hive.security", security)
                    .put("hive.max-partitions-per-scan", "1000")
                    .put("hive.assume-canonical-partition-keys", "true")
                    .build();
            Map<String, String> hiveBucketedProperties = ImmutableMap.<String, String>builder()
                    .putAll(hiveProperties)
                    .put("hive.max-initial-split-size", "10kB") // so that each bucket has multiple splits
                    .put("hive.max-split-size", "10kB") // so that each bucket has multiple splits
                    .put("hive.storage-format", "TEXTFILE") // so that there's no minimum split size for the file
                    .put("hive.compression-codec", "NONE") // so that the file is splittable
                    .build();
            queryRunner.createCatalog(HIVE_CATALOG, HIVE_CATALOG, hiveProperties);
            queryRunner.createCatalog(HIVE_BUCKETED_CATALOG, HIVE_CATALOG, hiveBucketedProperties);

            HiveIdentity identity = new HiveIdentity(SESSION);
            if (!metastore.getDatabase(TPCH_SCHEMA).isPresent()) {
                metastore.createDatabase(identity, createDatabaseMetastoreObject(TPCH_SCHEMA));
                copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, createSession(Optional.empty()), tables);
            }

            if (!metastore.getDatabase(TPCH_BUCKETED_SCHEMA).isPresent()) {
                metastore.createDatabase(identity, createDatabaseMetastoreObject(TPCH_BUCKETED_SCHEMA));
                copyTpchTablesBucketed(queryRunner, "tpch", TINY_SCHEMA_NAME, createBucketedSession(Optional.empty()), tables);
            }

            return queryRunner;
        }
        catch (Exception e) {
            queryRunner.close();
            throw e;
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

    public static Session createSession(Optional<SelectedRole> role)
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

    public static void copyTpchTablesBucketed(
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
        Logging.initialize();

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

        DistributedQueryRunner queryRunner = createQueryRunner(TpchTable.getTables(), ImmutableMap.of("http-server.http.port", "8080"), baseDataDir);
        Thread.sleep(10);
        Logger log = Logger.get(DistributedQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
