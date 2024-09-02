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
import io.trino.spi.ErrorType;
import io.trino.testing.BaseFailureRecoveryTest;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.execution.FailureInjector.FAILURE_INJECTION_MESSAGE;
import static io.trino.execution.FailureInjector.InjectedFailureType.TASK_FAILURE;
import static io.trino.execution.FailureInjector.InjectedFailureType.TASK_GET_RESULTS_REQUEST_FAILURE;
import static io.trino.execution.FailureInjector.InjectedFailureType.TASK_GET_RESULTS_REQUEST_TIMEOUT;
import static io.trino.execution.FailureInjector.InjectedFailureType.TASK_MANAGEMENT_REQUEST_FAILURE;
import static io.trino.execution.FailureInjector.InjectedFailureType.TASK_MANAGEMENT_REQUEST_TIMEOUT;
import static io.trino.operator.RetryPolicy.TASK;

public abstract class BaseIcebergFailureRecoveryTest
        extends BaseFailureRecoveryTest
{
    protected BaseIcebergFailureRecoveryTest(RetryPolicy retryPolicy)
    {
        super(retryPolicy);
    }

    @Override
    protected boolean areWriteRetriesSupported()
    {
        return true;
    }

    @Test
    protected void testCreatePartitionedTable()
    {
        testTableModification(
                Optional.empty(),
                "CREATE TABLE <table> WITH (partitioning = ARRAY['p']) AS SELECT *, 'partition1' p FROM orders",
                Optional.of("DROP TABLE <table>"));
    }

    // Copied from BaseDeltaFailureRecoveryTest
    @Test
    @Override
    protected void testDelete()
    {
        // Test method is overridden because method from superclass assumes more complex plan for `DELETE` query.
        // Assertions do not play well if plan consists of just two fragments.

        Optional<String> setupQuery = Optional.of("CREATE TABLE <table> AS SELECT * FROM orders");
        Optional<String> cleanupQuery = Optional.of("DROP TABLE <table>");
        String deleteQuery = "DELETE FROM <table> WHERE orderkey = 1";

        if (getRetryPolicy() == TASK) {
            assertThatQuery(deleteQuery)
                    .withSetupQuery(setupQuery)
                    .withCleanupQuery(cleanupQuery)
                    .experiencing(TASK_FAILURE, Optional.of(ErrorType.INTERNAL_ERROR))
                    .at(boundaryCoordinatorStage())
                    .finishesSuccessfully()
                    .cleansUpTemporaryTables();
        }
        else {
            assertThatQuery(deleteQuery)
                    .withSetupQuery(setupQuery)
                    .withCleanupQuery(cleanupQuery)
                    .experiencing(TASK_FAILURE, Optional.of(ErrorType.INTERNAL_ERROR))
                    .at(boundaryCoordinatorStage())
                    .failsAlways(failure -> failure.hasMessageContaining(FAILURE_INJECTION_MESSAGE))
                    .cleansUpTemporaryTables();
        }

        if (getRetryPolicy() == TASK) {
            assertThatQuery(deleteQuery)
                    .withSetupQuery(setupQuery)
                    .withCleanupQuery(cleanupQuery)
                    .experiencing(TASK_FAILURE, Optional.of(ErrorType.INTERNAL_ERROR))
                    .at(rootStage())
                    .finishesSuccessfully()
                    .cleansUpTemporaryTables();
        }
        else {
            assertThatQuery(deleteQuery)
                    .withSetupQuery(setupQuery)
                    .withCleanupQuery(cleanupQuery)
                    .experiencing(TASK_FAILURE, Optional.of(ErrorType.INTERNAL_ERROR))
                    .at(rootStage())
                    .failsAlways(failure -> failure.hasMessageContaining(FAILURE_INJECTION_MESSAGE))
                    .cleansUpTemporaryTables();
        }

        assertThatQuery(deleteQuery)
                .withSetupQuery(setupQuery)
                .withCleanupQuery(cleanupQuery)
                .experiencing(TASK_FAILURE, Optional.of(ErrorType.INTERNAL_ERROR))
                .at(leafStage())
                .failsWithoutRetries(failure -> failure.hasMessageContaining(FAILURE_INJECTION_MESSAGE))
                .finishesSuccessfully()
                .cleansUpTemporaryTables();

        // note: this is effectively same as test with `leafStage`. Should it be dropped?
        assertThatQuery(deleteQuery)
                .withSetupQuery(setupQuery)
                .withCleanupQuery(cleanupQuery)
                .experiencing(TASK_FAILURE, Optional.of(ErrorType.INTERNAL_ERROR))
                .at(boundaryDistributedStage())
                .failsWithoutRetries(failure -> failure.hasMessageContaining(FAILURE_INJECTION_MESSAGE))
                .finishesSuccessfully()
                .cleansUpTemporaryTables();

        assertThatQuery(deleteQuery)
                .withSetupQuery(setupQuery)
                .withCleanupQuery(cleanupQuery)
                .experiencing(TASK_MANAGEMENT_REQUEST_FAILURE)
                .at(boundaryDistributedStage())
                .failsWithoutRetries(failure -> failure.hasMessageFindingMatch("Error 500 Internal Server Error|Error closing remote buffer, expected 204 got 500"))
                .finishesSuccessfully()
                .cleansUpTemporaryTables();

        assertThatQuery(deleteQuery)
                .withSetupQuery(setupQuery)
                .withCleanupQuery(cleanupQuery)
                .experiencing(TASK_MANAGEMENT_REQUEST_TIMEOUT)
                .at(boundaryDistributedStage())
                .failsWithoutRetries(failure -> failure.hasMessageFindingMatch("Encountered too many errors talking to a worker node|Error closing remote buffer"))
                .finishesSuccessfully()
                .cleansUpTemporaryTables();

        if (getRetryPolicy() == RetryPolicy.QUERY) {
            assertThatQuery(deleteQuery)
                    .withSetupQuery(setupQuery)
                    .withCleanupQuery(cleanupQuery)
                    .experiencing(TASK_GET_RESULTS_REQUEST_FAILURE)
                    .at(boundaryDistributedStage())
                    .failsWithoutRetries(failure -> failure.hasMessageFindingMatch("Error 500 Internal Server Error|Error closing remote buffer, expected 204 got 500"))
                    .finishesSuccessfully()
                    .cleansUpTemporaryTables();

            assertThatQuery(deleteQuery)
                    .withSetupQuery(setupQuery)
                    .withCleanupQuery(cleanupQuery)
                    .experiencing(TASK_GET_RESULTS_REQUEST_TIMEOUT)
                    .at(boundaryDistributedStage())
                    .failsWithoutRetries(failure -> failure.hasMessageFindingMatch("Encountered too many errors talking to a worker node|Error closing remote buffer"))
                    .finishesSuccessfully()
                    .cleansUpTemporaryTables();
        }
    }

    // Copied from BaseDeltaFailureRecoveryTest
    @Test
    @Override
    protected void testUpdate()
    {
        // Test method is overridden because method from superclass assumes more complex plan for `UPDATE` query.
        // Assertions do not play well if plan consists of just two fragments.

        Optional<String> setupQuery = Optional.of("CREATE TABLE <table> AS SELECT * FROM orders");
        Optional<String> cleanupQuery = Optional.of("DROP TABLE <table>");
        String updateQuery = "UPDATE <table> SET shippriority = 101 WHERE custkey = 1";

        if (getRetryPolicy() == TASK) {
            assertThatQuery(updateQuery)
                    .withSetupQuery(setupQuery)
                    .withCleanupQuery(cleanupQuery)
                    .experiencing(TASK_FAILURE, Optional.of(ErrorType.INTERNAL_ERROR))
                    .at(boundaryCoordinatorStage())
                    .finishesSuccessfully()
                    .cleansUpTemporaryTables();
        }
        else {
            assertThatQuery(updateQuery)
                    .withSetupQuery(setupQuery)
                    .withCleanupQuery(cleanupQuery)
                    .experiencing(TASK_FAILURE, Optional.of(ErrorType.INTERNAL_ERROR))
                    .at(boundaryCoordinatorStage())
                    .failsAlways(failure -> failure.hasMessageContaining(FAILURE_INJECTION_MESSAGE))
                    .cleansUpTemporaryTables();
        }

        if (getRetryPolicy() == TASK) {
            assertThatQuery(updateQuery)
                    .withSetupQuery(setupQuery)
                    .withCleanupQuery(cleanupQuery)
                    .experiencing(TASK_FAILURE, Optional.of(ErrorType.INTERNAL_ERROR))
                    .at(rootStage())
                    .finishesSuccessfully()
                    .cleansUpTemporaryTables();
        }
        else {
            assertThatQuery(updateQuery)
                    .withSetupQuery(setupQuery)
                    .withCleanupQuery(cleanupQuery)
                    .experiencing(TASK_FAILURE, Optional.of(ErrorType.INTERNAL_ERROR))
                    .at(rootStage())
                    .failsAlways(failure -> failure.hasMessageContaining(FAILURE_INJECTION_MESSAGE))
                    .cleansUpTemporaryTables();
        }

        assertThatQuery(updateQuery)
                .withSetupQuery(setupQuery)
                .withCleanupQuery(cleanupQuery)
                .experiencing(TASK_FAILURE, Optional.of(ErrorType.INTERNAL_ERROR))
                .at(leafStage())
                .failsWithoutRetries(failure -> failure.hasMessageContaining(FAILURE_INJECTION_MESSAGE))
                .finishesSuccessfully()
                .cleansUpTemporaryTables();

        assertThatQuery(updateQuery)
                .withSetupQuery(setupQuery)
                .withCleanupQuery(cleanupQuery)
                .experiencing(TASK_FAILURE, Optional.of(ErrorType.INTERNAL_ERROR))
                .at(boundaryDistributedStage())
                .failsWithoutRetries(failure -> failure.hasMessageContaining(FAILURE_INJECTION_MESSAGE))
                .finishesSuccessfully()
                .cleansUpTemporaryTables();

        assertThatQuery(updateQuery)
                .withSetupQuery(setupQuery)
                .withCleanupQuery(cleanupQuery)
                .experiencing(TASK_MANAGEMENT_REQUEST_FAILURE)
                .at(boundaryDistributedStage())
                .failsWithoutRetries(failure -> failure.hasMessageFindingMatch("Error 500 Internal Server Error|Error closing remote buffer, expected 204 got 500"))
                .finishesSuccessfully()
                .cleansUpTemporaryTables();

        assertThatQuery(updateQuery)
                .withSetupQuery(setupQuery)
                .withCleanupQuery(cleanupQuery)
                .experiencing(TASK_MANAGEMENT_REQUEST_TIMEOUT)
                .at(boundaryDistributedStage())
                .failsWithoutRetries(failure -> failure.hasMessageFindingMatch("Encountered too many errors talking to a worker node|Error closing remote buffer"))
                .finishesSuccessfully();

        if (getRetryPolicy() == RetryPolicy.QUERY) {
            assertThatQuery(updateQuery)
                    .withSetupQuery(setupQuery)
                    .withCleanupQuery(cleanupQuery)
                    .experiencing(TASK_GET_RESULTS_REQUEST_FAILURE)
                    .at(boundaryDistributedStage())
                    .failsWithoutRetries(failure -> failure.hasMessageFindingMatch("Error 500 Internal Server Error|Error closing remote buffer, expected 204 got 500"))
                    .finishesSuccessfully()
                    .cleansUpTemporaryTables();

            assertThatQuery(updateQuery)
                    .withSetupQuery(setupQuery)
                    .withCleanupQuery(cleanupQuery)
                    .experiencing(TASK_GET_RESULTS_REQUEST_TIMEOUT)
                    .at(boundaryDistributedStage())
                    .failsWithoutRetries(failure -> failure.hasMessageFindingMatch("Encountered too many errors talking to a worker node|Error closing remote buffer"))
                    .finishesSuccessfully()
                    .cleansUpTemporaryTables();
        }
    }

    @Test
    protected void testInsertIntoNewPartition()
    {
        testTableModification(
                Optional.of("CREATE TABLE <table> WITH (partitioning = ARRAY['p']) AS SELECT *, 'partition1' p FROM orders"),
                "INSERT INTO <table> SELECT *, 'partition2' p FROM orders",
                Optional.of("DROP TABLE <table>"));
    }

    @Test
    protected void testInsertIntoExistingPartition()
    {
        testTableModification(
                Optional.of("CREATE TABLE <table> WITH (partitioning = ARRAY['p']) AS SELECT *, 'partition1' p FROM orders"),
                "INSERT INTO <table> SELECT *, 'partition1' p FROM orders",
                Optional.of("DROP TABLE <table>"));
    }

    @Test
    protected void testMergePartitionedTable()
    {
        testTableModification(
                Optional.of("CREATE TABLE <table> WITH (partitioning = ARRAY['bucket(orderkey, 10)']) AS SELECT * FROM orders"),
                """
                        MERGE INTO <table> t
                        USING (SELECT orderkey, 'X' clerk FROM <table>) s
                        ON t.orderkey = s.orderkey
                        WHEN MATCHED AND s.orderkey > 1000
                            THEN UPDATE SET clerk = t.clerk || s.clerk
                        WHEN MATCHED AND s.orderkey <= 1000
                            THEN DELETE
                        """,
                Optional.of("DROP TABLE <table>"));
    }
}
