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
package io.trino.plugin.jdbc;

import io.airlift.testing.EquivalenceTester;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.plugin.jdbc.MetadataUtil.COLUMN_CODEC;
import static io.trino.plugin.jdbc.MetadataUtil.assertJsonRoundTrip;
import static io.trino.plugin.jdbc.TestingJdbcTypeHandle.JDBC_BIGINT;
import static io.trino.plugin.jdbc.TestingJdbcTypeHandle.JDBC_VARCHAR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;

public class TestJdbcColumnHandle
{
    @Test
    public void testJsonRoundTrip()
    {
        assertJsonRoundTrip(
                COLUMN_CODEC,
                JdbcColumnHandle.builder()
                        .setColumnName("columnName")
                        .setJdbcTypeHandle(JDBC_VARCHAR)
                        .setColumnType(VARCHAR)
                        .setComment(Optional.of("some comment"))
                        .build());
    }

    @Test
    public void testEquivalence()
    {
        EquivalenceTester.equivalenceTester()
                .addEquivalentGroup(
                        new JdbcColumnHandle("columnName", JDBC_VARCHAR, VARCHAR),
                        new JdbcColumnHandle("columnName", JDBC_VARCHAR, VARCHAR),
                        new JdbcColumnHandle("columnName", JDBC_BIGINT, BIGINT),
                        new JdbcColumnHandle("columnName", JDBC_VARCHAR, VARCHAR))
                .addEquivalentGroup(
                        new JdbcColumnHandle("columnNameX", JDBC_VARCHAR, VARCHAR),
                        new JdbcColumnHandle("columnNameX", JDBC_VARCHAR, VARCHAR),
                        new JdbcColumnHandle("columnNameX", JDBC_BIGINT, BIGINT),
                        new JdbcColumnHandle("columnNameX", JDBC_VARCHAR, VARCHAR))
                .check();
    }
}
