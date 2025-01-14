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

import io.trino.plugin.hive.HiveTimestampPrecision;
import io.trino.tempto.BeforeMethodWithContext;
import io.trino.tempto.ProductTest;
import io.trino.tempto.assertions.QueryAssert;
import org.testng.annotations.Test;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;

import static io.trino.plugin.hive.HiveTimestampPrecision.MICROSECONDS;
import static io.trino.plugin.hive.HiveTimestampPrecision.MILLISECONDS;
import static io.trino.plugin.hive.HiveTimestampPrecision.NANOSECONDS;
import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.testing.SystemEnvironmentUtils.requireEnv;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static io.trino.tests.product.TestGroups.HIVE4;
import static io.trino.tests.product.TestGroups.PROFILE_SPECIFIC_TESTS;
import static io.trino.tests.product.utils.QueryExecutors.onHive;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHiveLegacyDateCompatibility
        extends ProductTest
{
    private static final String TRINO_CATALOG = "hive";

    private String bucketName;

    @BeforeMethodWithContext
    public void setUp()
    {
        bucketName = requireEnv("S3_BUCKET");
    }

    @Test(groups = {HIVE4, PROFILE_SPECIFIC_TESTS})
    public void testHiveParquetLegacyDateCompatibility()
    {
        testHiveLegacyDateCompatibility("PARQUET");
    }

    @Test(groups = {HIVE4, PROFILE_SPECIFIC_TESTS})
    public void testHiveOrcLegacyDateCompatibility()
    {
        testHiveLegacyDateCompatibility("ORC");
    }

    private void testHiveLegacyDateCompatibility(String format)
    {
        String hiveTableName = "test_hive_%s_legacy_date_compatibility_%s".formatted(format.toLowerCase(ENGLISH), randomNameSuffix());
        String trinoTableName = format("%s.default.%s", TRINO_CATALOG, hiveTableName);

        try {
            if (format.equalsIgnoreCase("PARQUET")) {
                assertThat(onHive().executeQuery("SET hive.parquet.date.proleptic.gregorian;")).containsOnly(List.of(row("hive.parquet.date.proleptic.gregorian=false")));
            }
            onHive().executeQuery(format("CREATE TABLE default.%s (value integer, date_col date) STORED AS %s LOCATION 's3://%s/%s'", hiveTableName, format, bucketName, hiveTableName));
            onHive().executeQuery(format("""
                    INSERT INTO %s VALUES
                        (1, '2022-04-13'),
                        (2, '1584-09-15'),
                        (3, '1584-09-10'),
                        (4, '1584-09-05'),
                        (5, '0001-01-01'),
                        (6, '1001-01-01'),
                        (7, '1234-01-01')""", hiveTableName));

            List<QueryAssert.Row> expectedRows = List.of(
                    row(1, Date.valueOf("2022-04-13")),
                    row(2, Date.valueOf("1584-09-15")),
                    row(3, Date.valueOf("1584-09-10")),
                    row(4, Date.valueOf("1584-09-05")),
                    row(5, Date.valueOf("0001-01-01")),
                    row(6, Date.valueOf("1001-01-01")),
                    row(7, Date.valueOf("1234-01-01")));

            assertThat(onHive().executeQuery("SELECT value, date_col FROM " + hiveTableName))
                    .containsOnly(expectedRows);

            assertThat(onTrino().executeQuery("SELECT value, date_col FROM " + trinoTableName))
                    .containsOnly(expectedRows);
        }
        finally {
            onHive().executeQuery("DROP TABLE IF EXISTS " + hiveTableName);
        }
    }

    @Test(groups = {HIVE4, PROFILE_SPECIFIC_TESTS})
    public void testHiveParquetLegacyTimestampCompatibility()
    {
        testHiveLegacyTimestampCompatibility("PARQUET", MILLISECONDS, "123");
        testHiveLegacyTimestampCompatibility("PARQUET", MICROSECONDS, "123456");
        testHiveLegacyTimestampCompatibility("PARQUET", NANOSECONDS, "123456789");
    }

    @Test(groups = {HIVE4, PROFILE_SPECIFIC_TESTS})
    public void testHiveOrcLegacyTimestampCompatibility()
    {
        testHiveLegacyTimestampCompatibility("ORC", MILLISECONDS, "123");
        testHiveLegacyTimestampCompatibility("ORC", MICROSECONDS, "123456");
        testHiveLegacyTimestampCompatibility("ORC", NANOSECONDS, "123456789");
    }

    private void testHiveLegacyTimestampCompatibility(String format, HiveTimestampPrecision hiveTimestampPrecision, String fractionalPart)
    {
        String hiveTableName = "test_hive_%s_legacy_tmst_compatibility_%s".formatted(format.toLowerCase(ENGLISH), randomNameSuffix());
        String trinoTableName = format("%s.default.%s", TRINO_CATALOG, hiveTableName);

        try {
            assertThat(onHive().executeQuery("SET hive.parquet.date.proleptic.gregorian;")).containsOnly(List.of(row("hive.parquet.date.proleptic.gregorian=false")));
            onTrino().executeQuery("SET SESSION hive.timestamp_precision='%s'".formatted(hiveTimestampPrecision));
            onHive().executeQuery(format("CREATE TABLE default.%s (tmst timestamp) STORED AS %s LOCATION 's3://%s/%s'", hiveTableName, format, bucketName, hiveTableName));
            onHive().executeQuery(format("INSERT INTO %s VALUES ('2022-04-13 15:30:12.%s')", hiveTableName, fractionalPart));
            onHive().executeQuery(format("INSERT INTO %s VALUES ('1584-09-15 15:30:12.%s')", hiveTableName, fractionalPart));
            onHive().executeQuery(format("INSERT INTO %s VALUES ('1584-09-10 15:30:12.%s')", hiveTableName, fractionalPart));
            onHive().executeQuery(format("INSERT INTO %s VALUES ('1584-09-05 15:30:12.%s')", hiveTableName, fractionalPart));
            onHive().executeQuery(format("INSERT INTO %s VALUES ('0001-01-01 15:30:12.%s')", hiveTableName, fractionalPart));
            onHive().executeQuery(format("INSERT INTO %s VALUES ('1001-01-01 15:30:12.%s')", hiveTableName, fractionalPart));
            onHive().executeQuery(format("INSERT INTO %s VALUES ('1234-01-01 15:30:12.%s')", hiveTableName, fractionalPart));

            List<QueryAssert.Row> expectedRows = List.of(
                    row(Timestamp.valueOf("2022-04-13 15:30:12.%s".formatted(fractionalPart))),
                    row(Timestamp.valueOf("1584-09-15 15:30:12.%s".formatted(fractionalPart))),
                    row(Timestamp.valueOf("1584-09-10 15:30:12.%s".formatted(fractionalPart))),
                    row(Timestamp.valueOf("1584-09-05 15:30:12.%s".formatted(fractionalPart))),
                    row(Timestamp.valueOf("0001-01-01 15:30:12.%s".formatted(fractionalPart))),
                    row(Timestamp.valueOf("1001-01-01 15:30:12.%s".formatted(fractionalPart))),
                    row(Timestamp.valueOf("1234-01-01 15:30:12.%s".formatted(fractionalPart))));

            assertThat(onHive().executeQuery("SELECT tmst FROM " + hiveTableName))
                    .containsOnly(expectedRows);

            assertThat(onTrino().executeQuery("SELECT tmst FROM " + trinoTableName))
                    .containsOnly(expectedRows);
        }
        finally {
            onHive().executeQuery("DROP TABLE IF EXISTS " + hiveTableName);
        }
    }
}
