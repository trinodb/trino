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
package io.trino.plugin.starrocks;

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;

import static org.assertj.core.api.Assertions.assertThat;

@EnabledIfSystemProperty(named = "starrocks.test.integration.enabled", matches = "true")
@Execution(ExecutionMode.SAME_THREAD)
final class TestStarRocksIntegrationSmokeTest
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return StarRocksIntegrationQueryRunner.createQueryRunner();
    }

    @Test
    void testShowSchemas()
    {
        assertQuery("SHOW SCHEMAS FROM starrocks LIKE 'starrocks_test'", "VALUES 'starrocks_test'");
    }

    @Test
    void testShowTablesAndDescribe()
    {
        assertQuery(
                "SHOW TABLES FROM starrocks.starrocks_test",
                "VALUES 'events', 'events_view', 'starrocks_specific'");
        assertQuery(
                "DESCRIBE starrocks.starrocks_test.events",
                """
                VALUES
                    ('id', 'bigint', '', ''),
                    ('created_at', 'timestamp(0)', '', ''),
                    ('name', 'varchar(20)', '', ''),
                    ('amount', 'decimal(18,2)', '', '')
                """);
    }

    @Test
    void testShowCreateTable()
    {
        assertThat((String) computeActual("SHOW CREATE TABLE starrocks.starrocks_test.events").getOnlyValue())
                .contains("CREATE TABLE starrocks.starrocks_test.events")
                .contains("created_at timestamp(0)")
                .contains("name varchar(20)")
                .contains("amount decimal(18, 2)");
    }

    @Test
    void testCountAndDatetimeReads()
    {
        assertQuery("SELECT count(*) FROM starrocks.starrocks_test.events", "VALUES 3");
        assertQuery(
                "SELECT sum(id), avg(id), min(id), max(created_at) FROM starrocks.starrocks_test.events WHERE id >= 2",
                "VALUES (5, 2.5, 2, TIMESTAMP '2024-01-03 12:17:32')");
        assertQuery(
                "SELECT id, created_at FROM starrocks.starrocks_test.events ORDER BY id",
                """
                VALUES
                    (1, TIMESTAMP '2024-01-01 10:15:30'),
                    (2, TIMESTAMP '2024-01-02 11:16:31'),
                    (3, TIMESTAMP '2024-01-03 12:17:32')
                """);
    }

    @Test
    void testViewsAndSpecificTypeFallbacks()
    {
        assertQuery(
                "SELECT * FROM starrocks.starrocks_test.events_view ORDER BY id",
                "VALUES (1, 'alpha'), (2, 'beta'), (3, 'gamma')");
        assertQuery(
                "SELECT metric_key, largeint_summary FROM starrocks.starrocks_test.starrocks_specific ORDER BY metric_key",
                "VALUES (10, '9223372036854775809'), (11, '-1')");
    }
}
