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

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.plugin.jdbc.MetadataUtil.OUTPUT_TABLE_CODEC;
import static io.trino.plugin.jdbc.MetadataUtil.assertJsonRoundTrip;
import static io.trino.plugin.jdbc.TestingJdbcTypeHandle.JDBC_VARCHAR;
import static io.trino.spi.type.VarcharType.VARCHAR;

public class TestJdbcOutputTableHandle
{
    @Test
    public void testJsonRoundTrip()
    {
        JdbcOutputTableHandle handleForCreate = new JdbcOutputTableHandle(
                "catalog",
                "schema",
                "table",
                ImmutableList.of("abc", "xyz"),
                ImmutableList.of(VARCHAR, VARCHAR),
                Optional.empty(),
                Optional.of("tmp_table"),
                Optional.of("page_sink_id"));

        assertJsonRoundTrip(OUTPUT_TABLE_CODEC, handleForCreate);

        JdbcOutputTableHandle handleForInsert = new JdbcOutputTableHandle(
                "catalog",
                "schema",
                "table",
                ImmutableList.of("abc", "xyz"),
                ImmutableList.of(VARCHAR, VARCHAR),
                Optional.of(ImmutableList.of(JDBC_VARCHAR, JDBC_VARCHAR)),
                Optional.of("tmp_table"),
                Optional.of("page_sink_id"));

        assertJsonRoundTrip(OUTPUT_TABLE_CODEC, handleForInsert);
    }
}
