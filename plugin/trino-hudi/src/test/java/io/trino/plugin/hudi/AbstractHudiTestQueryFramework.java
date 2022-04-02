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

package io.trino.plugin.hudi;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.hive.HivePlugin;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.type.TimeZoneKey;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;

import java.nio.file.Path;
import java.util.Locale;
import java.util.Map;

import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;

public class AbstractHudiTestQueryFramework
        extends AbstractTestQueryFramework
{
    public static final String HUDI_CATALOG = "hudi";
    public static final String HIVE_CATALOG = "hive";
    public static final String HUDI_SCHEMA = "default";

    static final String NON_PARTITIONED_TABLE_NAME = "hudi_non_part_cow";
    static final String PARTITIONED_COW_TABLE_NAME = "stock_ticks_cow";
    static final String PARTITIONED_MOR_TABLE_NAME = "stock_ticks_mor";

    private static final String CREATE_NON_PARTITIONED_TABLE_STATEMENT = "CREATE TABLE %s.\"%s\".\"%s\" (\n" +
            "    _hoodie_commit_time varchar,\n" +
            "    _hoodie_commit_seqno varchar,\n" +
            "    _hoodie_record_key varchar,\n" +
            "    _hoodie_partition_path varchar,\n" +
            "    _hoodie_file_name varchar,\n" +
            "    rowid varchar,\n" +
            "    partitionid varchar,\n" +
            "    precomb bigint,\n" +
            "    name varchar,\n" +
            "    versionid varchar,\n" +
            "    tobedeletedstr varchar,\n" +
            "    inttolong integer,\n" +
            "    longtoint bigint\n" +
            " )\n" +
            " WITH (\n" +
            "    external_location = '%s',\n" +
            "    format = 'PARQUET'\n" +
            " )";

    private static final String CREATE_PARTITIONED_TABLE_STATEMENT = "CREATE TABLE %s.\"%s\".\"%s\" (\n" +
            "    _hoodie_commit_time varchar,\n" +
            "    _hoodie_commit_seqno varchar,\n" +
            "    _hoodie_record_key varchar,\n" +
            "    _hoodie_partition_path varchar,\n" +
            "    _hoodie_file_name varchar,\n" +
            "    volume bigint,\n" +
            "    ts varchar,\n" +
            "    symbol varchar,\n" +
            "    year integer,\n" +
            "    month varchar,\n" +
            "    high double,\n" +
            "    low double,\n" +
            "    key varchar,\n" +
            "    date varchar,\n" +
            "    close double,\n" +
            "    open double,\n" +
            "    day varchar,\n" +
            "    dt varchar\n" +
            " )\n" +
            " WITH (\n" +
            "    external_location = '%s',\n" +
            "    format = 'PARQUET',\n" +
            "    partitioned_by = ARRAY['dt']\n" +
            " )";

    private static final Map<String, String> TABLE_NAME_TO_CREATE_STATEMENT = ImmutableMap.<String, String>builder()
            .put(NON_PARTITIONED_TABLE_NAME, CREATE_NON_PARTITIONED_TABLE_STATEMENT)
            .put(PARTITIONED_COW_TABLE_NAME, CREATE_PARTITIONED_TABLE_STATEMENT)
            .put(PARTITIONED_MOR_TABLE_NAME, CREATE_PARTITIONED_TABLE_STATEMENT)
            .buildOrThrow();

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createHudiQueryRunner(ImmutableMap.of());
    }

    protected void assertHudiQuery(String table, String testQuery, String expResults, boolean fail)
    {
        try {
            syncHudiTableInMetastore(table);
            if (!fail) {
                assertQuery(testQuery, expResults);
            }
            else {
                assertQueryFails(testQuery, expResults);
            }
        }
        finally {
            dropHudiTableFromMetastore(table);
        }
    }

    protected static String getTableBasePath(String tableName)
    {
        return AbstractHudiTestQueryFramework.class.getClassLoader().getResource(tableName).toString();
    }

    private static DistributedQueryRunner createHudiQueryRunner(Map<String, String> extraProperties)
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog(HUDI_CATALOG)
                .setSchema(HUDI_SCHEMA.toLowerCase(Locale.ROOT))
                .setTimeZoneKey(TimeZoneKey.UTC_KEY)
                .build();

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .setExtraProperties(extraProperties)
                .build();

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");

        Path dataDir = queryRunner.getCoordinator().getBaseDataDir().resolve("hudi_metadata");
        Path catalogDir = dataDir.getParent().resolve("catalog");

        // Install Hudi connector
        queryRunner.installPlugin(new HudiPlugin());
        Map<String, String> hudiProperties = ImmutableMap.<String, String>builder()
                .put("hive.metastore", "file")
                .put("hive.metastore.catalog.dir", catalogDir.toFile().toURI().toString())
                .buildOrThrow();
        queryRunner.createCatalog(HUDI_CATALOG, "hudi", hudiProperties);

        // Install Hive connector
        queryRunner.installPlugin(new HivePlugin());
        Map<String, String> hiveProperties = ImmutableMap.<String, String>builder()
                .put("hive.metastore", "file")
                .put("hive.metastore.catalog.dir", catalogDir.toFile().toURI().toString())
                .put("hive.allow-drop-table", "true")
                .put("hive.security", "legacy")
                .buildOrThrow();
        queryRunner.createCatalog(HIVE_CATALOG, "hive", hiveProperties);
        queryRunner.execute(format("CREATE SCHEMA %s.%s", HIVE_CATALOG, HUDI_SCHEMA));

        return queryRunner;
    }

    protected void syncHudiTableInMetastore(String tableName)
    {
        getQueryRunner().execute(format(
                TABLE_NAME_TO_CREATE_STATEMENT.get(tableName),
                HIVE_CATALOG,
                HUDI_SCHEMA,
                tableName,
                getTableBasePath(tableName)));
    }

    protected void dropHudiTableFromMetastore(String tableName)
    {
        getQueryRunner().execute(
                format("DROP TABLE IF EXISTS %s.\"%s\".\"%s\"", HIVE_CATALOG, HUDI_SCHEMA, tableName));
    }
}
