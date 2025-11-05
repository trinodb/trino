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

import io.trino.tempto.ProductTest;
import io.trino.tempto.assertions.QueryAssert;
import org.testng.annotations.Test;

import java.sql.Date;
import java.sql.Timestamp;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.HIVE4;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.utils.QueryExecutors.onHive;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHiveOnOrcLegacyDateCompatibility
        extends ProductTest
{
    private static final String TRINO_CATALOG = "hive";
    private static final String SCHEMA = "default";

    @Test(groups = {HIVE4, PROFILE_SPECIFIC_TESTS})
    public void testReadLegacyDateFromOrcWrittenByTrino()
    {
        String hiveTableName = "test_hive_orc_legacy_date_compatibility_%s".formatted(randomNameSuffix());
        String trinoTableName = format("%s.%s.%s", TRINO_CATALOG, SCHEMA, hiveTableName);

        try {
            onTrino().executeQuery("CREATE TABLE %s (date_col date, t timestamp) WITH (format = 'ORC')".formatted(trinoTableName));
            onTrino().executeQuery("""
                    INSERT INTO %s VALUES
                    (DATE '0002-01-01', TIMESTAMP '0002-01-01 00:00:00.123'),
                    (DATE '1500-01-01', TIMESTAMP '1500-01-01 00:00:00.123'),
                    (DATE '1582-10-04', TIMESTAMP '1582-10-04 00:00:00.123'),
                    (DATE '1582-11-04', TIMESTAMP '1582-11-04 00:00:00.123'),
                    (DATE '2000-02-29', TIMESTAMP '2000-02-29 00:00:00.123')""".formatted(trinoTableName));

            QueryAssert.Row[] expectedRows = {
                    row(Date.valueOf("0002-01-01"), Timestamp.valueOf("0002-01-01 00:00:00.123")),
                    row(Date.valueOf("1500-01-01"), Timestamp.valueOf("1500-01-01 00:00:00.123")),
                    row(Date.valueOf("1582-10-04"), Timestamp.valueOf("1582-10-04 00:00:00.123")),
                    row(Date.valueOf("1582-11-04"), Timestamp.valueOf("1582-11-04 00:00:00.123")),
                    row(Date.valueOf("2000-02-29"), Timestamp.valueOf("2000-02-29 00:00:00.123"))};

            assertThat(onTrino().executeQuery("SELECT date_col, t FROM " + trinoTableName)).containsOnly(expectedRows);
            assertThat(onHive().executeQuery("SELECT date_col, t  FROM " + hiveTableName)).containsOnly(expectedRows);
        }
        finally {
            onTrino().executeQuery("DROP TABLE IF EXISTS " + trinoTableName);
        }
    }

    @Test(groups = {HIVE4, PROFILE_SPECIFIC_TESTS})
    public void testReadLegacyDateFromOrcWrittenByHive()
    {
        String hiveTableName = "test_hive_orc_legacy_date_compatibility_%s".formatted(randomNameSuffix());
        String trinoTableName = format("%s.%s.%s", TRINO_CATALOG, SCHEMA, hiveTableName);

        try {
            onHive().executeQuery("CREATE TABLE %s (date_col date, t timestamp) STORED AS ORC".formatted(hiveTableName));
            onHive().executeQuery("""
                    INSERT INTO %s VALUES
                    ('0002-01-01', '0002-01-01 00:00:00.123'),
                    ('1500-01-01', '1500-01-01 00:00:00.123'),
                    ('1582-10-04', '1582-10-04 00:00:00.123'),
                    ('1582-11-04', '1582-11-04 00:00:00.123'),
                    ('2000-02-29', '2000-02-29 00:00:00.123')""".formatted(hiveTableName));

            QueryAssert.Row[] expectedRowsHive = {
                    row(Date.valueOf("0002-01-01"), Timestamp.valueOf("0002-01-01 00:00:00.123")),
                    row(Date.valueOf("1500-01-01"), Timestamp.valueOf("1500-01-01 00:00:00.123")),
                    row(Date.valueOf("1582-10-04"), Timestamp.valueOf("1582-10-04 00:00:00.123")),
                    row(Date.valueOf("1582-11-04"), Timestamp.valueOf("1582-11-04 00:00:00.123")),
                    row(Date.valueOf("2000-02-29"), Timestamp.valueOf("2000-02-29 00:00:00.123"))};
            assertThat(onHive().executeQuery("SELECT date_col, t FROM " + hiveTableName)).containsOnly(expectedRowsHive);

            // https://github.com/trinodb/trino/issues/26865
            QueryAssert.Row[] expectedRowsTrino = {
                    row(Date.valueOf("0001-12-30"), Timestamp.valueOf("0001-12-30 00:00:00.123")),
                    row(Date.valueOf("1500-01-10"), Timestamp.valueOf("1500-01-10 00:00:00.123")),
                    row(Date.valueOf("1582-10-24"), Timestamp.valueOf("1582-10-24 00:00:00.123")),
                    row(Date.valueOf("1582-11-04"), Timestamp.valueOf("1582-11-04 00:00:00.123")),
                    row(Date.valueOf("2000-02-29"), Timestamp.valueOf("2000-02-29 00:00:00.123"))};
            assertThat(onTrino().executeQuery("SELECT date_col, t FROM " + trinoTableName)).containsOnly(expectedRowsTrino);
        }
        finally {
            onHive().executeQuery("DROP TABLE IF EXISTS " + trinoTableName);
        }
    }
}
