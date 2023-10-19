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
package io.trino.plugin.redshift;

import io.trino.operator.RetryPolicy;
import org.testng.annotations.DataProvider;

import java.util.Optional;

import static io.trino.execution.FailureInjector.InjectedFailureType.TASK_GET_RESULTS_REQUEST_FAILURE;

public class TestRedshiftQueryFailureRecoverySmokeTest
        extends BaseRedshiftFailureRecoveryTest
{
    public TestRedshiftQueryFailureRecoverySmokeTest()
    {
        super(RetryPolicy.QUERY);
    }

    @Override
    @DataProvider(name = "parallelTests", parallel = true)
    public Object[][] parallelTests()
    {
        // Skip the regular FTE tests to execute the smoke test faster
        return new Object[][] {
                parallelTest("testCreateTableAsSelect", this::testCreateTableAsSelect),
        };
    }

    private void testCreateTableAsSelect()
    {
        assertThatQuery("CREATE TABLE <table> AS SELECT * FROM orders")
                .withCleanupQuery(Optional.of("DROP TABLE <table>"))
                .experiencing(TASK_GET_RESULTS_REQUEST_FAILURE)
                .at(boundaryDistributedStage())
                .failsWithoutRetries(failure -> failure.hasMessageFindingMatch("Error 500 Internal Server Error|Error closing remote buffer, expected 204 got 500"))
                .finishesSuccessfully()
                .cleansUpTemporaryTables();
    }
}
