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
package io.trino.tests.product.hive;

import io.trino.testing.containers.environment.ProductTest;
import io.trino.testing.containers.environment.RequiresEnvironment;
import io.trino.testing.containers.environment.Row;
import io.trino.tests.product.TestGroup;
import org.junit.jupiter.api.Test;

import java.sql.Date;
import java.sql.Timestamp;

import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.testing.containers.environment.QueryResultAssert.assertThat;
import static io.trino.testing.containers.environment.Row.row;
import static java.lang.String.format;

/**
 * Tests for Hive ORC legacy date compatibility.
 * <p>
 * Ported from the Tempto-based TestHiveOnOrcLegacyDateCompatibility.
 */
@ProductTest
@RequiresEnvironment(MultinodeHive4Environment.class)
@TestGroup.Hive4
class TestHiveOnOrcLegacyDateCompatibility
{
    private static final String TRINO_CATALOG = "hive";
    private static final String SCHEMA = "default";

    @Test
    void testReadLegacyDateFromOrcWrittenByTrino(MultinodeHive4Environment env)
    {
        String hiveTableName = "test_hive_orc_legacy_date_compatibility_%s".formatted(randomNameSuffix());
        String trinoTableName = format("%s.%s.%s", TRINO_CATALOG, SCHEMA, hiveTableName);

        try {
            env.executeTrinoUpdate("CREATE TABLE %s (date_col date, t timestamp) WITH (format = 'ORC')".formatted(trinoTableName));
            env.executeTrinoUpdate("""
                    INSERT INTO %s VALUES
                    (DATE '0002-01-01', TIMESTAMP '0002-01-01 00:00:00.123'),
                    (DATE '1500-01-01', TIMESTAMP '1500-01-01 00:00:00.123'),
                    (DATE '1582-10-04', TIMESTAMP '1582-10-04 00:00:00.123'),
                    (DATE '1582-11-04', TIMESTAMP '1582-11-04 00:00:00.123'),
                    (DATE '2000-02-29', TIMESTAMP '2000-02-29 00:00:00.123')""".formatted(trinoTableName));

            Row[] expectedRows = {
                    row(Date.valueOf("0002-01-01"), Timestamp.valueOf("0002-01-01 00:00:00.123")),
                    row(Date.valueOf("1500-01-01"), Timestamp.valueOf("1500-01-01 00:00:00.123")),
                    row(Date.valueOf("1582-10-04"), Timestamp.valueOf("1582-10-04 00:00:00.123")),
                    row(Date.valueOf("1582-11-04"), Timestamp.valueOf("1582-11-04 00:00:00.123")),
                    row(Date.valueOf("2000-02-29"), Timestamp.valueOf("2000-02-29 00:00:00.123"))};

            assertThat(env.executeTrino("SELECT date_col, t FROM " + trinoTableName)).containsOnly(expectedRows);
            assertThat(env.executeHive("SELECT date_col, t FROM " + hiveTableName)).containsOnly(expectedRows);
        }
        finally {
            env.executeTrinoUpdate("DROP TABLE IF EXISTS " + trinoTableName);
        }
    }

    @Test
    void testReadLegacyDateFromOrcWrittenByHive(MultinodeHive4Environment env)
    {
        String hiveTableName = "test_hive_orc_legacy_date_compatibility_%s".formatted(randomNameSuffix());
        String trinoTableName = format("%s.%s.%s", TRINO_CATALOG, SCHEMA, hiveTableName);

        try {
            env.executeHiveUpdate("CREATE TABLE %s (date_col date, t timestamp) STORED AS ORC".formatted(hiveTableName));
            env.executeHiveUpdate("""
                    INSERT INTO %s VALUES
                    ('0002-01-01', '0002-01-01 00:00:00.123'),
                    ('1500-01-01', '1500-01-01 00:00:00.123'),
                    ('1582-10-04', '1582-10-04 00:00:00.123'),
                    ('1582-11-04', '1582-11-04 00:00:00.123'),
                    ('2000-02-29', '2000-02-29 00:00:00.123')""".formatted(hiveTableName));

            Row[] expectedRowsHive = {
                    row(Date.valueOf("0002-01-01"), Timestamp.valueOf("0002-01-01 00:00:00.123")),
                    row(Date.valueOf("1500-01-01"), Timestamp.valueOf("1500-01-01 00:00:00.123")),
                    row(Date.valueOf("1582-10-04"), Timestamp.valueOf("1582-10-04 00:00:00.123")),
                    row(Date.valueOf("1582-11-04"), Timestamp.valueOf("1582-11-04 00:00:00.123")),
                    row(Date.valueOf("2000-02-29"), Timestamp.valueOf("2000-02-29 00:00:00.123"))};
            assertThat(env.executeHive("SELECT date_col, t FROM " + hiveTableName)).containsOnly(expectedRowsHive);

            // https://github.com/trinodb/trino/issues/26865
            Row[] expectedRowsTrino = {
                    row(Date.valueOf("0001-12-30"), Timestamp.valueOf("0001-12-30 00:00:00.123")),
                    row(Date.valueOf("1500-01-10"), Timestamp.valueOf("1500-01-10 00:00:00.123")),
                    row(Date.valueOf("1582-10-24"), Timestamp.valueOf("1582-10-24 00:00:00.123")),
                    row(Date.valueOf("1582-11-04"), Timestamp.valueOf("1582-11-04 00:00:00.123")),
                    row(Date.valueOf("2000-02-29"), Timestamp.valueOf("2000-02-29 00:00:00.123"))};
            assertThat(env.executeTrino("SELECT date_col, t FROM " + trinoTableName)).containsOnly(expectedRowsTrino);
        }
        finally {
            env.executeHiveUpdate("DROP TABLE IF EXISTS " + hiveTableName);
        }
    }
}
