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
package io.trino.plugin.singlestore;

import io.trino.testing.QueryRunner;
import io.trino.testing.datatype.SqlDataTypeTest;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.singlestore.TestingSingleStoreServer.LATEST_TESTED_TAG;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestSingleStoreLatestTypeMapping
        extends BaseSingleStoreTypeMapping
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        singleStoreServer = closeAfterClass(new TestingSingleStoreServer(LATEST_TESTED_TAG));
        return SingleStoreQueryRunner.builder(singleStoreServer).build();
    }

    @Test
    void testUnsupportedTinyint()
    {
        try (TestTable table = newTrinoTable("tpch.test_unsupported_tinyint", "(value tinyint)")) {
            assertThatThrownBy(() -> singleStoreServer.execute(format("INSERT INTO %s VALUES (-129)", table.getName())))
                    .hasMessageContaining("Out of range value");
            assertThatThrownBy(() -> singleStoreServer.execute(format("INSERT INTO %s VALUES (128)", table.getName())))
                    .hasMessageContaining("Out of range value");
        }
    }

    @Test
    void testUnsupportedSmallint()
    {
        try (TestTable table = newTrinoTable("tpch.test_unsupported_smallint", "(value smallint)")) {
            assertThatThrownBy(() -> singleStoreServer.execute(format("INSERT INTO %s VALUES (-32769)", table.getName())))
                    .hasMessageContaining("Out of range value");
            assertThatThrownBy(() -> singleStoreServer.execute(format("INSERT INTO %s VALUES (32768)", table.getName())))
                    .hasMessageContaining("Out of range value");
        }
    }

    @Test
    void testUnsupportedInteger()
    {
        try (TestTable table = newTrinoTable("tpch.test_unsupported_integer", "(value integer)")) {
            assertThatThrownBy(() -> singleStoreServer.execute(format("INSERT INTO %s VALUES (-2147483649)", table.getName())))
                    .hasMessageContaining("Out of range value");
            assertThatThrownBy(() -> singleStoreServer.execute(format("INSERT INTO %s VALUES (2147483648)", table.getName())))
                    .hasMessageContaining("Out of range value");
        }
    }

    @Test
    void testUnsupportedBigint()
    {
        try (TestTable table = newTrinoTable("tpch.test_unsupported_bigint", "(value bigint)")) {
            assertThatThrownBy(() -> singleStoreServer.execute(format("INSERT INTO %s VALUES (-9223372036854775809)", table.getName())))
                    .hasMessageContaining("Out of range value");
            assertThatThrownBy(() -> singleStoreServer.execute(format("INSERT INTO %s VALUES (9223372036854775808)", table.getName())))
                    .hasMessageContaining("Out of range value");
        }
    }

    @Test
    void testOlderDate()
    {
        try (TestTable table = new TestTable(singleStoreServer::execute, "tpch.test_unsupported_date", "(value date)")) {
            assertThatThrownBy(() -> singleStoreServer.execute(format("INSERT INTO %s VALUES (CAST('0000-01-01' AS date))", table.getName())))
                    .hasMessageContaining("Invalid DATE/TIME in type conversion");
            assertThatThrownBy(() -> singleStoreServer.execute(format("INSERT INTO %s VALUES (CAST('0001-01-01' AS date))", table.getName())))
                    .hasMessageContaining("Invalid DATE/TIME in type conversion");
        }
    }

    @Test
    void testSingleStoreCreatedParameterizedVarcharUnicodeEmoji()
    {
        try (TestTable table = new TestTable(singleStoreServer::execute, "tpch.test_unsupported_bigint", "(value varchar(1) CHARACTER SET utf8)")) {
            assertThatThrownBy(() -> singleStoreServer.execute(format("INSERT INTO %s VALUES ('😂')", table.getName())))
                    .hasMessageContaining("Data invalid");
        }

        SqlDataTypeTest.create()
                .addRoundTrip("varchar(1) CHARACTER SET utf8mb4", "'😂'", createVarcharType(1), "CAST('😂' AS varchar(1))")
                .execute(getQueryRunner(), singleStoreCreateAndInsert("tpch.singlestore_test_parameterized_varchar_unicode"));
    }
}
