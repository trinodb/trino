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
package io.trino.client.direct;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.blackhole.BlackHolePlugin;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Execution;

import java.util.concurrent.TimeUnit;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestDirectTrinoClient
{
    private QueryRunner queryRunner;

    @BeforeAll
    public void setup()
            throws Exception
    {
        queryRunner = new StandaloneQueryRunner(
                TEST_SESSION,
                builder -> builder.overrideProperties(ImmutableMap.of(
                        "query.client.timeout", "1s")));
        queryRunner.installPlugin(new BlackHolePlugin());
        queryRunner.createCatalog("blackhole", "blackhole");
        queryRunner.execute("CREATE SCHEMA blackhole.test_schema");
        queryRunner.execute("CREATE TABLE blackhole.test_schema.slow_test_table (col1 VARCHAR, col2 INTEGER)" +
                "WITH (" +
                "   split_count = 1, " +
                "   pages_per_split = 1, " +
                "   rows_per_page = 1, " +
                "   page_processing_delay = '3s'" +
                ")");
    }

    @Test
    @Timeout(value = 20, unit = TimeUnit.SECONDS)
    public void testDirectTrinoClientLongQuery()
    {
        queryRunner.execute(TEST_SESSION, "SELECT * FROM blackhole.test_schema.slow_test_table");
    }
}
