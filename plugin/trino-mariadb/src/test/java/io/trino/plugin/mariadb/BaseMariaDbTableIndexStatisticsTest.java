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
package io.trino.plugin.mariadb;

import io.trino.testing.MaterializedRow;
import org.testng.SkipException;
import org.testng.annotations.Test;

import static java.lang.String.format;

public abstract class BaseMariaDbTableIndexStatisticsTest
        extends BaseMariaDbTableStatisticsTest
{
    protected BaseMariaDbTableIndexStatisticsTest(String dockerImageName)
    {
        super(
                dockerImageName,
                nullFraction -> 0.1, // Without mysql.column_stats we have no way of knowing real null fraction, 10% is just a "wild guess"
                varcharNdv -> null); // Without mysql.column_stats we don't know cardinality for varchar columns
    }

    @Override
    protected void gatherStats(String tableName)
    {
        for (MaterializedRow row : computeActual("SHOW COLUMNS FROM " + tableName)) {
            String columnName = (String) row.getField(0);
            String columnType = (String) row.getField(1);
            if (columnType.startsWith("varchar")) {
                continue;
            }
            executeInMariaDb(format("CREATE INDEX %2$s ON %1$s (%2$s)", tableName, columnName).replace("\"", "`"));
        }
        executeInMariaDb("ANALYZE TABLE " + tableName.replace("\"", "`"));
    }

    @Test
    @Override
    public void testStatsWithPredicatePushdownWithStatsPrecalculationDisabled()
    {
        // TODO (https://github.com/trinodb/trino/issues/11664) implement the test for MariaDB, with permissive approximate assertions
        throw new SkipException("Test to be implemented");
    }

    @Test
    @Override
    public void testStatsWithPredicatePushdown()
    {
        // TODO (https://github.com/trinodb/trino/issues/11664) implement the test for MariaDB, with permissive approximate assertions
        throw new SkipException("Test to be implemented");
    }

    @Test
    @Override
    public void testStatsWithVarcharPredicatePushdown()
    {
        // TODO (https://github.com/trinodb/trino/issues/11664) implement the test for MariaDB, with permissive approximate assertions
        throw new SkipException("Test to be implemented");
    }

    @Test
    @Override
    public void testStatsWithLimitPushdown()
    {
        // TODO (https://github.com/trinodb/trino/issues/11664) implement the test for MariaDB, with permissive approximate assertions
        throw new SkipException("Test to be implemented");
    }

    @Test
    @Override
    public void testStatsWithTopNPushdown()
    {
        // TODO (https://github.com/trinodb/trino/issues/11664) implement the test for MariaDB, with permissive approximate assertions
        throw new SkipException("Test to be implemented");
    }

    @Test
    @Override
    public void testStatsWithDistinctPushdown()
    {
        // TODO (https://github.com/trinodb/trino/issues/11664) implement the test for MariaDB, with permissive approximate assertions
        throw new SkipException("Test to be implemented");
    }

    @Test
    @Override
    public void testStatsWithDistinctLimitPushdown()
    {
        // TODO (https://github.com/trinodb/trino/issues/11664) implement the test for MariaDB, with permissive approximate assertions
        throw new SkipException("Test to be implemented");
    }

    @Test
    @Override
    public void testStatsWithAggregationPushdown()
    {
        // TODO (https://github.com/trinodb/trino/issues/11664) implement the test for MariaDB, with permissive approximate assertions
        throw new SkipException("Test to be implemented");
    }

    @Test
    @Override
    public void testStatsWithSimpleJoinPushdown()
    {
        // TODO (https://github.com/trinodb/trino/issues/11664) implement the test for MariaDB, with permissive approximate assertions
        throw new SkipException("Test to be implemented");
    }

    @Test
    @Override
    public void testStatsWithJoinPushdown()
    {
        // TODO (https://github.com/trinodb/trino/issues/11664) implement the test for MariaDB, with permissive approximate assertions
        throw new SkipException("Test to be implemented");
    }
}
