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
package io.trino.plugin.hive;

import io.trino.Session;
import io.trino.operator.RetryPolicy;
import io.trino.testing.BaseFailureRecoveryTest;
import org.testng.annotations.Test;

import java.util.Optional;

public abstract class BaseHiveFailureRecoveryTest
        extends BaseFailureRecoveryTest
{
    protected BaseHiveFailureRecoveryTest(RetryPolicy retryPolicy)
    {
        super(retryPolicy);
    }

    @Override
    protected boolean areWriteRetriesSupported()
    {
        return true;
    }

    @Test(invocationCount = INVOCATION_COUNT)
    public void testInsertIntoNewPartition()
    {
        testTableModification(
                Optional.of("CREATE TABLE <table> WITH (partitioned_by = ARRAY['p']) AS SELECT *, 'partition1' p FROM orders"),
                "INSERT INTO <table> SELECT *, 'partition2' p FROM orders",
                Optional.of("DROP TABLE <table>"));
    }

    @Test(invocationCount = INVOCATION_COUNT)
    public void testInsertIntoExistingPartition()
    {
        testTableModification(
                Optional.of("CREATE TABLE <table> WITH (partitioned_by = ARRAY['p']) AS SELECT *, 'partition1' p FROM orders"),
                "INSERT INTO <table> SELECT *, 'partition1' p FROM orders",
                Optional.of("DROP TABLE <table>"));
    }

    @Test(invocationCount = INVOCATION_COUNT)
    public void testInsertIntoNewPartitionBucketed()
    {
        testTableModification(
                Optional.of("CREATE TABLE <table> WITH (partitioned_by = ARRAY['p'], bucketed_by = ARRAY['orderkey'], bucket_count = 4) AS SELECT *, 'partition1' p FROM orders"),
                "INSERT INTO <table> SELECT *, 'partition2' p FROM orders",
                Optional.of("DROP TABLE <table>"));
    }

    @Test(invocationCount = INVOCATION_COUNT)
    public void testInsertIntoExistingPartitionBucketed()
    {
        testTableModification(
                Optional.of("CREATE TABLE <table> WITH (partitioned_by = ARRAY['p'], bucketed_by = ARRAY['orderkey'], bucket_count = 4) AS SELECT *, 'partition1' p FROM orders"),
                "INSERT INTO <table> SELECT *, 'partition1' p FROM orders",
                Optional.of("DROP TABLE <table>"));
    }

    @Test(invocationCount = INVOCATION_COUNT)
    public void testReplaceExistingPartition()
    {
        testTableModification(
                Optional.of(Session.builder(getQueryRunner().getDefaultSession())
                        .setCatalogSessionProperty("hive", "insert_existing_partitions_behavior", "OVERWRITE")
                        .build()),
                Optional.of("CREATE TABLE <table> WITH (partitioned_by = ARRAY['p']) AS SELECT *, 'partition1' p FROM orders"),
                "INSERT INTO <table> SELECT *, 'partition1' p FROM orders",
                Optional.of("DROP TABLE <table>"));
    }
}
