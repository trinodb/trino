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
package io.trino.plugin.clickhouse;

import com.google.common.collect.ImmutableMap;
import io.trino.testing.QueryRunner;

import java.util.OptionalInt;

import static io.trino.plugin.clickhouse.ClickHouseQueryRunner.createClickHouseQueryRunner;
import static io.trino.plugin.clickhouse.TestingClickHouseServer.CLICKHOUSE_LATEST_IMAGE;

public class TestClickHouseLatestConnectorTest
        extends BaseClickHouseConnectorTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        this.clickhouseServer = closeAfterClass(new TestingClickHouseServer(CLICKHOUSE_LATEST_IMAGE));
        return createClickHouseQueryRunner(
                clickhouseServer,
                ImmutableMap.of(),
                ImmutableMap.<String, String>builder()
                        .put("clickhouse.map-string-as-varchar", "true")
                        .buildOrThrow(),
                REQUIRED_TPCH_TABLES);
    }

    @Override
    protected OptionalInt maxTableNameLength()
    {
        // The numeric value depends on file system
        return OptionalInt.of(255 - ".sql.detached".length());
    }

    @Override
    protected String errorMessageForCreateTableAsSelectNegativeDate(String date)
    {
        // Override because the DateTime range was expanded in version 21.4 and later
        return "Date must be between 1970-01-01 and 2149-06-06 in ClickHouse: " + date;
    }

    @Override
    protected String errorMessageForInsertNegativeDate(String date)
    {
        // Override because the DateTime range was expanded in version 21.4 and later
        return "Date must be between 1970-01-01 and 2149-06-06 in ClickHouse: " + date;
    }

    @Override
    protected String errorMessageForDateYearOfEraPredicate(String date)
    {
        // Override because the DateTime range was expanded in version 21.4 and later
        return "Date must be between 1970-01-01 and 2149-06-06 in ClickHouse: " + date;
    }
}
