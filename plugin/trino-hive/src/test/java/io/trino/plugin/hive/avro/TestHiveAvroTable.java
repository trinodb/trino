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
package io.trino.plugin.hive.avro;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.HiveQueryRunner;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.testng.annotations.Test;

import static io.trino.tpch.TpchTable.NATION;

public class TestHiveAvroTable
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.builder()
                .setHiveProperties(ImmutableMap.of("hive.avro.time-zone", "Asia/Kamchatka"))
                .setInitialTables(ImmutableList.of(NATION))
                .build();
    }

    @Test
    public void testWritingAndReadingTimestampFromAvroTable()
    {
        assertUpdate("CREATE TABLE test_avro (id BIGINT, ts TIMESTAMP) WITH (format = 'AVRO')");
        String timestamp = "2020-05-11T11:15:05";
        assertUpdate("INSERT INTO test_avro VALUES(1, from_iso8601_timestamp('" + timestamp + "'))", 1);
        assertQuery("SELECT ts FROM test_avro WHERE id = 1", "VALUES('" + timestamp + "')");
    }
}
