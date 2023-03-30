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
package io.trino.testing;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.operator.OperatorStats;
import io.trino.operator.RetryPolicy;
import io.trino.server.DynamicFilterService.DynamicFilterDomainStats;
import io.trino.server.DynamicFilterService.DynamicFiltersStats;
import io.trino.spi.ErrorType;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.SystemSessionProperties.ENABLE_DYNAMIC_FILTERING;
import static io.trino.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static io.trino.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static io.trino.execution.FailureInjector.FAILURE_INJECTION_MESSAGE;
import static io.trino.execution.FailureInjector.InjectedFailureType.TASK_FAILURE;
import static io.trino.execution.FailureInjector.InjectedFailureType.TASK_MANAGEMENT_REQUEST_FAILURE;
import static io.trino.execution.FailureInjector.InjectedFailureType.TASK_MANAGEMENT_REQUEST_TIMEOUT;
import static io.trino.spi.predicate.Domain.singleValue;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.OptimizerConfig.JoinDistributionType.PARTITIONED;
import static io.trino.sql.planner.OptimizerConfig.JoinReorderingStrategy.NONE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public abstract class ExtendedFailureRecoveryTest
        extends BaseFailureRecoveryTest
{
    private static final String PARTITIONED_LINEITEM = "partitioned_lineitem";

    protected ExtendedFailureRecoveryTest(RetryPolicy retryPolicy)
    {
        super(retryPolicy);
    }

    @BeforeClass
    public void initTables()
            throws Exception
    {
        // setup partitioned fact table for dynamic partition pruning
        createPartitionedLineitemTable(PARTITIONED_LINEITEM, ImmutableList.of("orderkey", "partkey", "suppkey"), "suppkey");
    }

    protected abstract void createPartitionedLineitemTable(String tableName, List<String> columns, String partitionColumn);

    @Override
    @DataProvider(name = "parallelTests", parallel = true)
    public Object[][] parallelTests()
    {
        return moreParallelTests(super.parallelTests(),
                parallelTest("testSimpleSelect", this::testSimpleSelect),
                parallelTest("testAggregation", this::testAggregation),
                parallelTest("testJoinDynamicFilteringDisabled", this::testJoinDynamicFilteringDisabled),
                parallelTest("testJoinDynamicFilteringEnabled", this::testJoinDynamicFilteringEnabled),
                parallelTest("testUserFailure", this::testUserFailure));
    }

    protected void testSimpleSelect()
    {
        testSelect("SELECT * FROM nation");
    }

    protected void testAggregation()
    {
        testSelect("SELECT orderStatus, count(*) FROM orders GROUP BY orderStatus");
    }

    protected void testJoinDynamicFilteringDisabled()
    {
        @Language("SQL") String selectQuery = "SELECT * FROM partitioned_lineitem JOIN supplier ON partitioned_lineitem.suppkey = supplier.suppkey " +
                "AND supplier.name = 'Supplier#000000001'";
        testSelect(selectQuery, Optional.of(enableDynamicFiltering(false)));
    }

    protected void testJoinDynamicFilteringEnabled()
    {
        @Language("SQL") String selectQuery = "SELECT * FROM partitioned_lineitem JOIN supplier ON partitioned_lineitem.suppkey = supplier.suppkey " +
                "AND supplier.name = 'Supplier#000000001'";
        testSelect(
                selectQuery,
                Optional.of(enableDynamicFiltering(true)),
                queryId -> {
                    DynamicFiltersStats dynamicFiltersStats = getDynamicFilteringStats(queryId);
                    assertThat(dynamicFiltersStats.getLazyDynamicFilters())
                            .as("Dynamic filter is missing")
                            .isEqualTo(1);
                    DynamicFilterDomainStats domainStats = getOnlyElement(dynamicFiltersStats.getDynamicFilterDomainStats());
                    assertThat(domainStats.getSimplifiedDomain())
                            .isEqualTo(singleValue(BIGINT, 1L).toString(getSession().toConnectorSession()));
                    OperatorStats probeStats = searchScanFilterAndProjectOperatorStats(queryId, getQualifiedTableName(PARTITIONED_LINEITEM));
                    // Currently, stats from all attempts are combined.
                    // Asserting on multiple of 615L as well in case the probe scan was completed twice
                    assertThat(probeStats.getInputPositions()).isIn(615L, 1230L);
                });
    }

    protected void testUserFailure()
    {
        // Some connectors have pushdowns enabled for arithmetic operations (like SqlServer),
        // so exception will come not from trino, but from datasource itself
        Session withoutPushdown = Session.builder(this.getSession())
                .setSystemProperty("allow_pushdown_into_connectors", "false")
                .build();

        assertThatThrownBy(() -> getQueryRunner().execute(withoutPushdown, "SELECT * FROM nation WHERE regionKey / nationKey - 1 = 0"))
                .hasMessageMatching("(?i).*Division by zero.*"); // some errors come back with different casing.

        assertThatQuery("SELECT * FROM nation")
                .experiencing(TASK_FAILURE, Optional.of(ErrorType.USER_ERROR))
                .at(leafStage())
                .failsAlways(failure -> failure.hasMessageContaining(FAILURE_INJECTION_MESSAGE));
    }

    @Override
    protected void testRequestTimeouts()
    {
        // extra test cases not covered by general timeout cases scattered around
        assertThatQuery("SELECT * FROM nation")
                .experiencing(TASK_MANAGEMENT_REQUEST_TIMEOUT)
                .at(leafStage())
                .failsWithoutRetries(failure -> failure.hasMessageContaining("Encountered too many errors talking to a worker node"))
                .finishesSuccessfully();

        assertThatQuery("SELECT * FROM nation")
                .experiencing(TASK_MANAGEMENT_REQUEST_TIMEOUT)
                .at(boundaryDistributedStage())
                .failsWithoutRetries(failure -> failure.hasMessageContaining("Encountered too many errors talking to a worker node"))
                .finishesSuccessfully();

        super.testRequestTimeouts();
    }

    @Override
    protected void testNonSelect(Optional<Session> session, Optional<String> setupQuery, String query, Optional<String> cleanupQuery, boolean writesData)
    {
        super.testNonSelect(session, setupQuery, query, cleanupQuery, writesData);

        assertThatQuery(query)
                .withSession(session)
                .withSetupQuery(setupQuery)
                .withCleanupQuery(cleanupQuery)
                .experiencing(TASK_FAILURE, Optional.of(ErrorType.INTERNAL_ERROR))
                .at(leafStage())
                .failsWithoutRetries(failure -> failure.hasMessageContaining(FAILURE_INJECTION_MESSAGE))
                .finishesSuccessfully();

        assertThatQuery(query)
                .withSession(session)
                .withSetupQuery(setupQuery)
                .withCleanupQuery(cleanupQuery)
                .experiencing(TASK_FAILURE, Optional.of(ErrorType.INTERNAL_ERROR))
                .at(intermediateDistributedStage())
                .failsWithoutRetries(failure -> failure.hasMessageContaining(FAILURE_INJECTION_MESSAGE))
                .finishesSuccessfully();

        assertThatQuery(query)
                .withSession(session)
                .withSetupQuery(setupQuery)
                .withCleanupQuery(cleanupQuery)
                .experiencing(TASK_MANAGEMENT_REQUEST_FAILURE)
                .at(boundaryDistributedStage())
                .failsWithoutRetries(failure -> failure.hasMessageFindingMatch("Error 500 Internal Server Error|Error closing remote buffer, expected 204 got 500"))
                .finishesSuccessfully();
    }

    private Session enableDynamicFiltering(boolean enabled)
    {
        Session defaultSession = getQueryRunner().getDefaultSession();
        return Session.builder(defaultSession)
                .setSystemProperty(ENABLE_DYNAMIC_FILTERING, Boolean.toString(enabled))
                .setSystemProperty(JOIN_REORDERING_STRATEGY, NONE.name())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, PARTITIONED.name())
                // Ensure probe side scan wait until DF is collected
                .setCatalogSessionProperty(defaultSession.getCatalog().orElseThrow(), "dynamic_filtering_wait_timeout", "1h")
                .build();
    }
}
