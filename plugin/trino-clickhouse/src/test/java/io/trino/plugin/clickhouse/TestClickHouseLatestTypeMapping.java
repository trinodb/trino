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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.testing.QueryRunner;
import io.trino.testing.datatype.SqlDataTypeTest;
import org.testng.annotations.DataProvider;

import static io.trino.plugin.clickhouse.ClickHouseQueryRunner.createClickHouseQueryRunner;
import static io.trino.plugin.clickhouse.TestingClickHouseServer.CLICKHOUSE_LATEST_IMAGE;
import static io.trino.spi.type.TimestampType.createTimestampType;

public class TestClickHouseLatestTypeMapping
        extends BaseClickHouseTypeMapping
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        clickhouseServer = closeAfterClass(new TestingClickHouseServer(CLICKHOUSE_LATEST_IMAGE));
        return createClickHouseQueryRunner(clickhouseServer, ImmutableMap.of(),
                ImmutableMap.<String, String>builder()
                        .put("metadata.cache-ttl", "10m")
                        .put("metadata.cache-missing", "true")
                        .buildOrThrow(),
                ImmutableList.of());
    }

    @DataProvider
    @Override
    public Object[][] clickHouseDateMinMaxValuesDataProvider()
    {
        // Override because the Date range was expanded in version 21.4 and later
        return new Object[][] {
                {"1970-01-01"}, // min value in ClickHouse
                {"2149-06-06"}, // max value in ClickHouse
        };
    }

    @DataProvider
    @Override
    public Object[][] unsupportedClickHouseDateValuesDataProvider()
    {
        // Override because the Date range was expanded in version 21.4 and later
        return new Object[][] {
                {"1969-12-31"}, // min - 1 day
                {"2149-06-07"}, // max + 1 day
        };
    }

    @Override
    protected SqlDataTypeTest unsupportedTimestampBecomeUnexpectedValueTest(String inputType)
    {
        // Override because insert DateTime '1969-12-31 23:59:59' directly in ClickHouse will
        // become '1970-01-01 00:00:00' in version 21.4 and later, however in versions prior
        // to 21.4 the value will become '1970-01-01 23:59:59'.
        return SqlDataTypeTest.create()
                .addRoundTrip(inputType, "'1969-12-31 23:59:59'", createTimestampType(0), "TIMESTAMP '1970-01-01 00:00:00'");
    }

    @DataProvider
    @Override
    public Object[][] clickHouseDateTimeMinMaxValuesDataProvider()
    {
        // Override because the DateTime range was expanded in version 21.4 and later
        return new Object[][] {
                {"1970-01-01 00:00:00"}, // min value in ClickHouse
                {"2106-02-07 06:28:15"}, // max value in ClickHouse
        };
    }

    @DataProvider
    @Override
    public Object[][] unsupportedTimestampDataProvider()
    {
        // Override because the DateTime range was expanded in version 21.4 and later
        return new Object[][] {
                {"1969-12-31 23:59:59"}, // min - 1 second
                {"2106-02-07 06:28:16"}, // max + 1 second
        };
    }
}
