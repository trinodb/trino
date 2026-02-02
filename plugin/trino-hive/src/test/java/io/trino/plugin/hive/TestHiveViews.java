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

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHiveViews
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = HiveQueryRunner.builder()
                .build();

        // extra catalog with NANOSECOND timestamp precision
        queryRunner.createCatalog(
                "hive_timestamp_nanos",
                "hive",
                ImmutableMap.of("hive.timestamp-precision", "NANOSECONDS"));
        queryRunner.execute("CREATE SCHEMA hive_timestamp_nanos.tpch");

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
                withTimestampPrecision(defaultSession, HiveTimestampPrecision.NANOSECONDS),
                "CREATE TABLE " + tableName + " AS SELECT TIMESTAMP '1990-01-02 12:13:14.123456789' ts",
                1);

        // Presto view created with config property set to MILLIS and session property not set
        String prestoViewNameDefault = "presto_view_ts_default_" + randomNameSuffix();
        assertUpdate(defaultSession, "CREATE VIEW " + prestoViewNameDefault + " AS SELECT *  FROM hive.tpch." + tableName);
        assertUpdate(defaultSession, "CREATE VIEW hive_timestamp_nanos.tpch." + prestoViewNameDefault + " AS SELECT *  FROM hive.tpch." + tableName);

        assertThat(query(defaultSession, "SELECT ts FROM " + prestoViewNameDefault)).matches("VALUES TIMESTAMP '1990-01-02 12:13:14.123'");

        assertThat(query(defaultSession, "SELECT ts  FROM hive_timestamp_nanos.tpch." + prestoViewNameDefault)).matches("VALUES TIMESTAMP '1990-01-02 12:13:14.123'");

        assertThat(query(millisSession, "SELECT ts FROM " + prestoViewNameDefault)).matches("VALUES TIMESTAMP '1990-01-02 12:13:14.123'");
        assertThat(query(millisSession, "SELECT ts FROM hive_timestamp_nanos.tpch." + prestoViewNameDefault)).matches("VALUES TIMESTAMP '1990-01-02 12:13:14.123'");

        assertThat(query(nanosSessions, "SELECT ts FROM " + prestoViewNameDefault)).matches("VALUES TIMESTAMP '1990-01-02 12:13:14.123'");

        assertThat(query(nanosSessions, "SELECT ts FROM hive_timestamp_nanos.tpch." + prestoViewNameDefault)).matches("VALUES TIMESTAMP '1990-01-02 12:13:14.123'");

        // Presto view created with config property set to MILLIS and session property set to NANOS
        String prestoViewNameNanos = "presto_view_ts_nanos_" + randomNameSuffix();
        assertUpdate(nanosSessions, "CREATE VIEW " + prestoViewNameNanos + " AS SELECT *  FROM hive.tpch." + tableName);
        assertUpdate(nanosSessions, "CREATE VIEW hive_timestamp_nanos.tpch." + prestoViewNameNanos + " AS SELECT *  FROM hive.tpch." + tableName);

        assertThat(query(defaultSession, "SELECT ts FROM " + prestoViewNameNanos)).matches("VALUES TIMESTAMP '1990-01-02 12:13:14.123000000'");

        assertThat(query(defaultSession, "SELECT ts FROM hive_timestamp_nanos.tpch." + prestoViewNameNanos)).matches("VALUES TIMESTAMP '1990-01-02 12:13:14.123000000'");

        assertThat(query(millisSession, "SELECT ts FROM " + prestoViewNameNanos)).matches("VALUES TIMESTAMP '1990-01-02 12:13:14.123000000'");

        assertThat(query(millisSession, "SELECT ts FROM hive_timestamp_nanos.tpch." + prestoViewNameNanos)).matches("VALUES TIMESTAMP '1990-01-02 12:13:14.123000000'");

        assertThat(query(nanosSessions, "SELECT ts FROM " + prestoViewNameNanos)).matches("VALUES TIMESTAMP '1990-01-02 12:13:14.123000000'");

        assertThat(query(nanosSessions, "SELECT ts FROM hive_timestamp_nanos.tpch." + prestoViewNameNanos)).matches("VALUES TIMESTAMP '1990-01-02 12:13:14.123000000'");
    }

    private Session withTimestampPrecision(Session session, HiveTimestampPrecision precision)
    {
        return Session.builder(session)
                .setCatalogSessionProperty(session.getCatalog().orElseThrow(), "timestamp_precision", precision.name())
                .build();
    }
}
