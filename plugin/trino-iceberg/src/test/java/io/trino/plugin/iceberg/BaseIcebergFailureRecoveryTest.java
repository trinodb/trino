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
package io.trino.plugin.iceberg;

import io.trino.operator.RetryPolicy;
import io.trino.testing.BaseFailureRecoveryTest;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.lang.String.format;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

public abstract class BaseIcebergFailureRecoveryTest
        extends BaseFailureRecoveryTest
{
    protected BaseIcebergFailureRecoveryTest(RetryPolicy retryPolicy, Map<String, String> exchangeManagerProperties)
    {
        super(retryPolicy, exchangeManagerProperties);
    }

    @Override
    protected boolean areWriteRetriesSupported()
    {
        return true;
    }

    @Override
    public void testAnalyzeStatistics()
    {
        assertThatThrownBy(super::testAnalyzeStatistics)
                .hasMessageContaining("This connector does not support analyze");
    }

    @Override
    public void testDelete()
    {
        assertThatThrownBy(super::testDelete)
                .hasMessageContaining("This connector only supports delete where one or more identity-transformed partitions are deleted entirely");
    }

    @Override
    public void testDeleteWithSubquery()
    {
        assertThatThrownBy(super::testDelete)
                .hasMessageContaining("This connector only supports delete where one or more identity-transformed partitions are deleted entirely");
    }

    @Override
    protected void createPartitionedLineitemTable(String tableName, List<String> columns, String partitionColumn)
    {
        @Language("SQL") String sql = format(
                "CREATE TABLE %s WITH (partitioning=array['%s']) AS SELECT %s FROM tpch.tiny.lineitem",
                tableName,
                partitionColumn,
                String.join(",", columns));
        getQueryRunner().execute(sql);
    }

    @Override
    public void testUpdate()
    {
        assertThatThrownBy(super::testUpdate)
                .hasMessageContaining("This connector does not support updates");
    }

    @Override
    public void testUpdateWithSubquery()
    {
        assertThatThrownBy(super::testUpdateWithSubquery)
                .hasMessageContaining("This connector does not support updates");
    }

    @Test(invocationCount = INVOCATION_COUNT)
    public void testCreatePartitionedTable()
    {
        testTableModification(
                Optional.empty(),
                "CREATE TABLE <table> WITH (partitioning = ARRAY['p']) AS SELECT *, 'partition1' p FROM orders",
                Optional.of("DROP TABLE <table>"));
    }

    @Test(invocationCount = INVOCATION_COUNT)
    public void testInsertIntoNewPartition()
    {
        testTableModification(
                Optional.of("CREATE TABLE <table> WITH (partitioning = ARRAY['p']) AS SELECT *, 'partition1' p FROM orders"),
                "INSERT INTO <table> SELECT *, 'partition2' p FROM orders",
                Optional.of("DROP TABLE <table>"));
    }

    @Test(invocationCount = INVOCATION_COUNT)
    public void testInsertIntoExistingPartition()
    {
        testTableModification(
                Optional.of("CREATE TABLE <table> WITH (partitioning = ARRAY['p']) AS SELECT *, 'partition1' p FROM orders"),
                "INSERT INTO <table> SELECT *, 'partition1' p FROM orders",
                Optional.of("DROP TABLE <table>"));
    }
}
