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
package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.sql.planner.Partitioning;
import io.trino.sql.planner.PartitioningScheme;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.iterative.rule.test.RuleAssert;
import io.trino.sql.planner.iterative.rule.test.RuleTester;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.plan.ValuesNode;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.airlift.testing.Closeables.closeAllRuntimeException;
import static io.trino.SystemSessionProperties.USE_PREFERRED_WRITE_PARTITIONING;
import static io.trino.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static io.trino.sql.planner.assertions.PlanMatchPattern.tableWriter;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.iterative.rule.test.RuleTester.defaultRuleTester;
import static io.trino.testing.TestingSession.testSessionBuilder;

public class TestPreferWritePartitioning
{
    private static final String MOCK_CATALOG = "mock_catalog";
    private static final String TEST_SCHEMA = "test_schema";

    private static final Session SESSION = testSessionBuilder()
            .setCatalog(MOCK_CATALOG)
            .setSchema(TEST_SCHEMA)
            .build();
    private static final Session SESSION_WITH_PREFERRED_PARTITIONING = testSessionBuilder()
            .setCatalog(MOCK_CATALOG)
            .setSchema(TEST_SCHEMA)
            .setSystemProperty(USE_PREFERRED_WRITE_PARTITIONING, "true")
            .build();

    private static final PlanMatchPattern SUCCESSFUL_MATCH = tableWriter(
            ImmutableList.of(),
            ImmutableList.of(),
            values(0));

    private RuleTester tester;

    @BeforeClass
    public void setUp()
    {
        tester = defaultRuleTester();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        closeAllRuntimeException(tester);
        tester = null;
    }

    @Test
    public void testPreferWritePartitioning()
    {
        assertPreferredPartitioning(
                Optional.of(new PartitioningScheme(
                        Partitioning.create(FIXED_HASH_DISTRIBUTION, ImmutableList.of()),
                        ImmutableList.of())))
                .withSession(SESSION_WITH_PREFERRED_PARTITIONING)
                .matches(SUCCESSFUL_MATCH);
    }

    @Test
    public void testDoesNotFire()
    {
        assertPreferredPartitioning(
                Optional.of(new PartitioningScheme(
                        Partitioning.create(FIXED_HASH_DISTRIBUTION, ImmutableList.of()),
                        ImmutableList.of())))
                .withSession(SESSION)
                .doesNotFire();
    }

    private RuleAssert assertPreferredPartitioning(Optional<PartitioningScheme> preferredPartitioningScheme)
    {
        return tester.assertThat(new PreferWritePartitioning())
                .on(builder -> builder.tableWriter(
                        ImmutableList.of(),
                        ImmutableList.of(),
                        Optional.empty(),
                        preferredPartitioningScheme,
                        Optional.empty(),
                        Optional.empty(),
                        new ValuesNode(new PlanNodeId("mock"), 0)));
    }
}
