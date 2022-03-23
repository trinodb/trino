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
package io.trino.plugin.deltalake;

import io.trino.operator.RetryPolicy;
import io.trino.spi.ErrorType;
import io.trino.testing.BaseFailureRecoveryTest;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static io.trino.execution.FailureInjector.FAILURE_INJECTION_MESSAGE;
import static io.trino.execution.FailureInjector.InjectedFailureType.TASK_FAILURE;
import static io.trino.execution.FailureInjector.InjectedFailureType.TASK_GET_RESULTS_REQUEST_FAILURE;
import static io.trino.execution.FailureInjector.InjectedFailureType.TASK_GET_RESULTS_REQUEST_TIMEOUT;
import static io.trino.execution.FailureInjector.InjectedFailureType.TASK_MANAGEMENT_REQUEST_FAILURE;
import static io.trino.execution.FailureInjector.InjectedFailureType.TASK_MANAGEMENT_REQUEST_TIMEOUT;
import static java.lang.String.format;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;

public abstract class BaseDeltaFailureRecoveryTest
        extends BaseFailureRecoveryTest
{
    protected BaseDeltaFailureRecoveryTest(RetryPolicy retryPolicy)
    {
        super(retryPolicy);
    }

    @Override
    protected boolean areWriteRetriesSupported()
    {
        return true;
    }

    @Override
    public void testDelete()
    {
        // Test method is overriden because method from superclass assumes more complex plan for `DELETE` query.
        // Assertions do not play well if plan consists of just two fragments.

        Optional<String> setupQuery = Optional.of("CREATE TABLE <table> AS SELECT * FROM orders");
        Optional<String> cleanupQuery = Optional.of("DROP TABLE <table>");
        String deleteQuery = "DELETE FROM <table> WHERE orderkey = 1";

        assertThatQuery(deleteQuery)
                .withSetupQuery(setupQuery)
                .withCleanupQuery(cleanupQuery)
                .experiencing(TASK_FAILURE, Optional.of(ErrorType.INTERNAL_ERROR))
                .at(boundaryCoordinatorStage())
                .failsAlways(failure -> failure.hasMessageContaining(FAILURE_INJECTION_MESSAGE));

        assertThatQuery(deleteQuery)
                .withSetupQuery(setupQuery)
                .withCleanupQuery(cleanupQuery)
                .experiencing(TASK_FAILURE, Optional.of(ErrorType.INTERNAL_ERROR))
                .at(rootStage())
                .failsAlways(failure -> failure.hasMessageContaining(FAILURE_INJECTION_MESSAGE));

        assertThatQuery(deleteQuery)
                .withSetupQuery(setupQuery)
                .withCleanupQuery(cleanupQuery)
                .experiencing(TASK_FAILURE, Optional.of(ErrorType.INTERNAL_ERROR))
                .at(leafStage())
                .failsWithoutRetries(failure -> failure.hasMessageContaining(FAILURE_INJECTION_MESSAGE))
                .finishesSuccessfully();

        // note: this is effectively same as test with `leafStage`. Should it be dropped?
        assertThatQuery(deleteQuery)
                .withSetupQuery(setupQuery)
                .withCleanupQuery(cleanupQuery)
                .experiencing(TASK_FAILURE, Optional.of(ErrorType.INTERNAL_ERROR))
                .at(boundaryDistributedStage())
                .failsWithoutRetries(failure -> failure.hasMessageContaining(FAILURE_INJECTION_MESSAGE))
                .finishesSuccessfully();

        // DELETE plan is too simplistic for testing with `intermediateDistributedStage`
        assertThatThrownBy(() ->
                assertThatQuery(deleteQuery)
                        .withSetupQuery(setupQuery)
                        .withCleanupQuery(cleanupQuery)
                        .experiencing(TASK_FAILURE, Optional.of(ErrorType.INTERNAL_ERROR))
                        .at(intermediateDistributedStage())
                        .failsWithoutRetries(failure -> failure.hasMessageContaining(FAILURE_INJECTION_MESSAGE)))
                .hasMessageContaining("stage not found");

        assertThatQuery(deleteQuery)
                .withSetupQuery(setupQuery)
                .withCleanupQuery(cleanupQuery)
                .experiencing(TASK_MANAGEMENT_REQUEST_FAILURE)
                .at(boundaryDistributedStage())
                .failsWithoutRetries(failure -> failure.hasMessageFindingMatch("Error 500 Internal Server Error|Error closing remote buffer, expected 204 got 500"))
                .finishesSuccessfully();

        assertThatQuery(deleteQuery)
                .withSetupQuery(setupQuery)
                .withCleanupQuery(cleanupQuery)
                .experiencing(TASK_GET_RESULTS_REQUEST_FAILURE)
                .at(boundaryDistributedStage())
                .failsWithoutRetries(failure -> failure.hasMessageFindingMatch("Error 500 Internal Server Error|Error closing remote buffer, expected 204 got 500"))
                .finishesSuccessfully();

        assertThatQuery(deleteQuery)
                .withSetupQuery(setupQuery)
                .withCleanupQuery(cleanupQuery)
                .experiencing(TASK_MANAGEMENT_REQUEST_TIMEOUT)
                .at(boundaryDistributedStage())
                .failsWithoutRetries(failure -> failure.hasMessageContaining("Encountered too many errors talking to a worker node"))
                .finishesSuccessfully();

        assertThatQuery(deleteQuery)
                .withSetupQuery(setupQuery)
                .withCleanupQuery(cleanupQuery)
                .experiencing(TASK_GET_RESULTS_REQUEST_TIMEOUT)
                .at(boundaryDistributedStage())
                .failsWithoutRetries(failure -> failure.hasMessageContaining("Encountered too many errors talking to a worker node"))
                .finishesSuccessfully();
    }

    @Override
    public void testUpdate()
    {
        // Test method is overriden because method from superclass assumes more complex plan for `UPDATE` query.
        // Assertions do not play well if plan consists of just two fragments.

        Optional<String> setupQuery = Optional.of("CREATE TABLE <table> AS SELECT * FROM orders");
        Optional<String> cleanupQuery = Optional.of("DROP TABLE <table>");
        String updateQuery = "UPDATE <table> SET shippriority = 101 WHERE custkey = 1";

        assertThatQuery(updateQuery)
                .withSetupQuery(setupQuery)
                .withCleanupQuery(cleanupQuery)
                .experiencing(TASK_FAILURE, Optional.of(ErrorType.INTERNAL_ERROR))
                .at(boundaryCoordinatorStage())
                .failsAlways(failure -> failure.hasMessageContaining(FAILURE_INJECTION_MESSAGE));

        assertThatQuery(updateQuery)
                .withSetupQuery(setupQuery)
                .withCleanupQuery(cleanupQuery)
                .experiencing(TASK_FAILURE, Optional.of(ErrorType.INTERNAL_ERROR))
                .at(rootStage())
                .failsAlways(failure -> failure.hasMessageContaining(FAILURE_INJECTION_MESSAGE));

        assertThatQuery(updateQuery)
                .withSetupQuery(setupQuery)
                .withCleanupQuery(cleanupQuery)
                .experiencing(TASK_FAILURE, Optional.of(ErrorType.INTERNAL_ERROR))
                .at(leafStage())
                .failsWithoutRetries(failure -> failure.hasMessageContaining(FAILURE_INJECTION_MESSAGE))
                .finishesSuccessfully();

        assertThatQuery(updateQuery)
                .withSetupQuery(setupQuery)
                .withCleanupQuery(cleanupQuery)
                .experiencing(TASK_FAILURE, Optional.of(ErrorType.INTERNAL_ERROR))
                .at(boundaryDistributedStage())
                .failsWithoutRetries(failure -> failure.hasMessageContaining(FAILURE_INJECTION_MESSAGE))
                .finishesSuccessfully();

        // UPDATE plan is too simplistic for testing with `intermediateDistributedStage`
        assertThatThrownBy(() ->
                assertThatQuery(updateQuery)
                        .withSetupQuery(setupQuery)
                        .withCleanupQuery(cleanupQuery)
                        .experiencing(TASK_FAILURE, Optional.of(ErrorType.INTERNAL_ERROR))
                        .at(intermediateDistributedStage())
                        .failsWithoutRetries(failure -> failure.hasMessageContaining(FAILURE_INJECTION_MESSAGE)))
                .hasMessageContaining("stage not found");

        assertThatQuery(updateQuery)
                .withSetupQuery(setupQuery)
                .withCleanupQuery(cleanupQuery)
                .experiencing(TASK_MANAGEMENT_REQUEST_FAILURE)
                .at(boundaryDistributedStage())
                .failsWithoutRetries(failure -> failure.hasMessageFindingMatch("Error 500 Internal Server Error|Error closing remote buffer, expected 204 got 500"))
                .finishesSuccessfully();

        assertThatQuery(updateQuery)
                .withSetupQuery(setupQuery)
                .withCleanupQuery(cleanupQuery)
                .experiencing(TASK_GET_RESULTS_REQUEST_FAILURE)
                .at(boundaryDistributedStage())
                .failsWithoutRetries(failure -> failure.hasMessageFindingMatch("Error 500 Internal Server Error|Error closing remote buffer, expected 204 got 500"))
                .finishesSuccessfully();

        assertThatQuery(updateQuery)
                .withSetupQuery(setupQuery)
                .withCleanupQuery(cleanupQuery)
                .experiencing(TASK_MANAGEMENT_REQUEST_TIMEOUT)
                .at(boundaryDistributedStage())
                .failsWithoutRetries(failure -> failure.hasMessageContaining("Encountered too many errors talking to a worker node"))
                .finishesSuccessfully();

        assertThatQuery(updateQuery)
                .withSetupQuery(setupQuery)
                .withCleanupQuery(cleanupQuery)
                .experiencing(TASK_GET_RESULTS_REQUEST_TIMEOUT)
                .at(boundaryDistributedStage())
                .failsWithoutRetries(failure -> failure.hasMessageContaining("Encountered too many errors talking to a worker node"))
                .finishesSuccessfully();
    }

    @Override
    // materialized views are currently not implemented by Delta connector
    public void testRefreshMaterializedView()
    {
        assertThatThrownBy(super::testRefreshMaterializedView)
                .hasMessageContaining("This connector does not support creating materialized views");
    }

    @Override
    protected void createPartitionedLineitemTable(String tableName, List<String> columns, String partitionColumn)
    {
        String sql = format(
                "CREATE TABLE %s WITH (partitioned_by = array['%s']) AS SELECT %s FROM tpch.tiny.lineitem",
                tableName,
                partitionColumn,
                String.join(",", columns));
        getQueryRunner().execute(sql);
    }

    @Test(invocationCount = INVOCATION_COUNT)
    public void testCreatePartitionedTable()
    {
        testTableModification(
                Optional.empty(),
                "CREATE TABLE <table> WITH (partitioned_by = ARRAY['p']) AS SELECT *, 'partition1' p FROM orders",
                Optional.of("DROP TABLE <table>"));
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
}
