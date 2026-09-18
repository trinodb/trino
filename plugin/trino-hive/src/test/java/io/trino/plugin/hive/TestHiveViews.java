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

import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import java.nio.file.Path;
import java.util.Map;

import static com.google.common.base.Verify.verify;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

@Execution(SAME_THREAD) // Two catalogs use FileHiveMetastore, which operates on disk in non-thread safe manner
public class TestHiveViews
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(testSessionBuilder().build()).build();

        Path localFilesystemRoot = queryRunner.getCoordinator().getBaseDataDir().resolve("hive_data");
        verify(localFilesystemRoot.toFile().mkdirs());

        queryRunner.installPlugin(new TestingHivePlugin(localFilesystemRoot));

        String metastoreDir = "local:///hive-metastore";
        queryRunner.createCatalog(
                "hive",
                "hive",
                Map.of("hive.metastore.catalog.dir", metastoreDir));
        queryRunner.execute("CREATE SCHEMA hive.default");

        // extra catalog with NANOSECOND timestamp precision
        queryRunner.createCatalog(
                "hive_timestamp_nanos",
                "hive",
                Map.of(
                        "hive.metastore.catalog.dir", metastoreDir,
                        "hive.timestamp-precision", "NANOSECONDS"));

        return queryRunner;
    }

    @Test
    public void testViewTimestampPrecision()
    {
        Session defaultSession = getSession();
        Session millisSession = Session.builder(defaultSession)
                .setCatalogSessionProperty("hive", "timestamp_precision", "MILLISECONDS")
                .setCatalogSessionProperty("hive_timestamp_nanos", "timestamp_precision", "MILLISECONDS")
                .build();
        Session nanosSessions = Session.builder(defaultSession)
                .setCatalogSessionProperty("hive", "timestamp_precision", "NANOSECONDS")
                .setCatalogSessionProperty("hive_timestamp_nanos", "timestamp_precision", "NANOSECONDS")
                .build();

        // Hive views tests covered in TestHiveViews.testTimestampHiveView and TestHiveViesLegacy.testTimestampHiveView
        String tableName = "ts_hive_table_" + randomNameSuffix();
        assertUpdate(
                withTimestampPrecision(defaultSession, "hive", HiveTimestampPrecision.NANOSECONDS),
                "CREATE TABLE hive.default." + tableName + " AS SELECT TIMESTAMP '1990-01-02 12:13:14.123456789' ts",
                1);

        // Presto view created with config property set to MILLIS and session property not set
        String prestoViewNameDefault = "presto_view_ts_default_" + randomNameSuffix();
        assertUpdate(defaultSession, "CREATE VIEW hive.default." + prestoViewNameDefault + " AS SELECT *  FROM hive.default." + tableName);

        assertThat(query(defaultSession, "SELECT ts FROM hive.default." + prestoViewNameDefault)).matches("VALUES TIMESTAMP '1990-01-02 12:13:14.123'");

        assertThat(query(defaultSession, "SELECT ts  FROM hive_timestamp_nanos.default." + prestoViewNameDefault)).matches("VALUES TIMESTAMP '1990-01-02 12:13:14.123'");

        assertThat(query(millisSession, "SELECT ts FROM hive.default." + prestoViewNameDefault)).matches("VALUES TIMESTAMP '1990-01-02 12:13:14.123'");
        assertThat(query(millisSession, "SELECT ts FROM hive_timestamp_nanos.default." + prestoViewNameDefault)).matches("VALUES TIMESTAMP '1990-01-02 12:13:14.123'");

        assertThat(query(nanosSessions, "SELECT ts FROM hive.default." + prestoViewNameDefault)).matches("VALUES TIMESTAMP '1990-01-02 12:13:14.123'");

        assertThat(query(nanosSessions, "SELECT ts FROM hive_timestamp_nanos.default." + prestoViewNameDefault)).matches("VALUES TIMESTAMP '1990-01-02 12:13:14.123'");

        // Presto view created with config property set to MILLIS and session property set to NANOS
        String prestoViewNameNanos = "presto_view_ts_nanos_" + randomNameSuffix();
        assertUpdate(nanosSessions, "CREATE VIEW hive.default." + prestoViewNameNanos + " AS SELECT *  FROM hive.default." + tableName);

        assertThat(query(defaultSession, "SELECT ts FROM hive.default." + prestoViewNameNanos)).matches("VALUES TIMESTAMP '1990-01-02 12:13:14.123000000'");

        assertThat(query(defaultSession, "SELECT ts FROM hive_timestamp_nanos.default." + prestoViewNameNanos)).matches("VALUES TIMESTAMP '1990-01-02 12:13:14.123000000'");

        assertThat(query(millisSession, "SELECT ts FROM hive.default." + prestoViewNameNanos)).matches("VALUES TIMESTAMP '1990-01-02 12:13:14.123000000'");

        assertThat(query(millisSession, "SELECT ts FROM hive_timestamp_nanos.default." + prestoViewNameNanos)).matches("VALUES TIMESTAMP '1990-01-02 12:13:14.123000000'");

        assertThat(query(nanosSessions, "SELECT ts FROM hive.default." + prestoViewNameNanos)).matches("VALUES TIMESTAMP '1990-01-02 12:13:14.123000000'");

        assertThat(query(nanosSessions, "SELECT ts FROM hive_timestamp_nanos.default." + prestoViewNameNanos)).matches("VALUES TIMESTAMP '1990-01-02 12:13:14.123000000'");
    }

    private Session withTimestampPrecision(Session session, String inCatalog, HiveTimestampPrecision precision)
    {
        return Session.builder(session)
                .setCatalogSessionProperty(inCatalog, "timestamp_precision", precision.name())
                .build();
    }
}
