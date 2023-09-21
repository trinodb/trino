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
package io.trino.plugin.hive.s3;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.trino.Session;
import io.trino.plugin.hive.containers.HiveHadoop;
import io.trino.plugin.hive.containers.HiveMinioDataLake;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import io.trino.testing.sql.TestTable;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static java.lang.String.format;

public class TestMinioS3SelectQueries
        extends AbstractTestQueryFramework
{
    private static final String HIVE_TEST_SCHEMA = "hive_datalake";
    private static final DataSize HIVE_S3_STREAMING_PART_SIZE = DataSize.of(5, MEGABYTE);

    private String bucketName;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.bucketName = "test-hive-insert-overwrite-" + randomNameSuffix();
        HiveMinioDataLake hiveMinioDataLake = closeAfterClass(new HiveMinioDataLake(bucketName, HiveHadoop.HIVE3_IMAGE));
        hiveMinioDataLake.start();
        return S3HiveQueryRunner.builder(hiveMinioDataLake)
                .setHiveProperties(
                        ImmutableMap.<String, String>builder()
                                .put("hive.non-managed-table-writes-enabled", "true")
                                .put("hive.metastore-cache-ttl", "1d")
                                .put("hive.metastore-refresh-interval", "1d")
                                .put("hive.s3.streaming.part-size", HIVE_S3_STREAMING_PART_SIZE.toString())
                                .buildOrThrow())
                .build();
    }

    @BeforeClass
    public void setUp()
    {
        computeActual(format(
                "CREATE SCHEMA hive.%1$s WITH (location='s3a://%2$s/%1$s')",
                HIVE_TEST_SCHEMA,
                bucketName));
    }

    @Test
    public void testTextfileQueries()
    {
        // Demonstrate correctness issues which have resulted in pushdown for TEXTFILE
        // using CSV support in S3 Select being put behind a separate "experimental" flag.
        // TODO: https://github.com/trinodb/trino/issues/17775
        List<String> values = ImmutableList.of(
                "1, true, 11",
                "2, true, 22",
                "3, NULL, NULL",
                "4, false, 44");
        Session withS3SelectPushdown = Session.builder(getSession())
                .setCatalogSessionProperty("hive", "s3_select_pushdown_enabled", "true")
                .setCatalogSessionProperty("hive", "json_native_reader_enabled", "false")
                .setCatalogSessionProperty("hive", "text_file_native_reader_enabled", "false")
                .build();
        Session withoutS3SelectPushdown = Session.builder(getSession())
                .setCatalogSessionProperty("hive", "json_native_reader_enabled", "false")
                .setCatalogSessionProperty("hive", "text_file_native_reader_enabled", "false")
                .build();
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "hive.%s.test_textfile_queries".formatted(HIVE_TEST_SCHEMA),
                "(id INT, bool_t BOOLEAN, int_t INT) WITH (format = 'TEXTFILE')",
                values)) {
            assertQuery(withS3SelectPushdown, "SELECT id FROM " + table.getName() + " WHERE int_t IS NULL", "VALUES 3");
            assertQuery(withS3SelectPushdown, "SELECT id FROM " + table.getName() + " WHERE bool_t = true", "VALUES 1, 2");
        }

        List<String> specialCharacterValues = ImmutableList.of(
                "1, 'a,comma'",
                "2, 'a|pipe'",
                "3, 'an''escaped quote'",
                "4, 'a~null encoding'");
        try (TestTable table = new TestTable(
                getQueryRunner()::execute,
                "hive.%s.test_s3_select_pushdown_special_characters".formatted(HIVE_TEST_SCHEMA),
                "(id INT, string_t VARCHAR) WITH (format = 'TEXTFILE', textfile_field_separator=',', textfile_field_separator_escape='|', null_format='~')",
                specialCharacterValues)) {
            String selectWithComma = "SELECT id FROM " + table.getName() + " WHERE string_t = 'a,comma'";
            assertQuery(withoutS3SelectPushdown, selectWithComma, "VALUES 1");
            assertQuery(withS3SelectPushdown, selectWithComma, "VALUES 1");

            String selectWithPipe = "SELECT id FROM " + table.getName() + " WHERE string_t = 'a|pipe'";
            assertQuery(withoutS3SelectPushdown, selectWithPipe, "VALUES 2");
            assertQuery(withS3SelectPushdown, selectWithPipe, "VALUES 2");

            String selectWithQuote = "SELECT id FROM " + table.getName() + " WHERE string_t = 'an''escaped quote'";
            assertQuery(withoutS3SelectPushdown, selectWithQuote, "VALUES 3");
            assertQuery(withS3SelectPushdown, selectWithQuote, "VALUES 3");

            String selectWithNullFormatEncoding = "SELECT id FROM " + table.getName() + " WHERE string_t = 'a~null encoding'";
            assertQuery(withoutS3SelectPushdown, selectWithNullFormatEncoding, "VALUES 4");
            assertQuery(withS3SelectPushdown, selectWithNullFormatEncoding, "VALUES 4");
        }
    }
}
