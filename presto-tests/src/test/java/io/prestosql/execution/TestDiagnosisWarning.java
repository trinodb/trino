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
package io.prestosql.execution;

import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.execution.warnings.DefaultWarningCollector;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.execution.warnings.WarningCollectorConfig;
import io.prestosql.plugin.tpch.TpchConnectorFactory;
import io.prestosql.spi.PrestoWarning;
import io.prestosql.spi.WarningCode;
import io.prestosql.spi.connector.StandardWarningCode;
import io.prestosql.sql.planner.LogicalPlanner;
import io.prestosql.testing.LocalQueryRunner;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.fail;

public class TestDiagnosisWarning
{
    private LocalQueryRunner queryRunner;

    @BeforeClass
    public void setUp()
    {
        queryRunner = new LocalQueryRunner(testSessionBuilder()
                .setCatalog("local")
                .setSchema("tiny")
                .build());

        queryRunner.createCatalog(
                queryRunner.getDefaultSession().getCatalog().get(),
                new TpchConnectorFactory(1),
                ImmutableMap.of());
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        queryRunner.close();
    }

    @Test
    public void testWarningSession()
    {
        WarningCode warningCode = StandardWarningCode.INTEGER_DIVISION.toWarningCode();
        // sessionProperties = null
        assertPlannerWarnings(queryRunner,
                "SELECT 1/2",
                ImmutableMap.of(),
                Optional.empty());

        // session enable_query_diagnosis_warning = true
        assertPlannerWarnings(queryRunner,
                "SELECT 1/2",
                Optional.of(warningCode));
    }

    @Test
    public void testIntegerDivisionWarning()
    {
        WarningCode warningCode = StandardWarningCode.INTEGER_DIVISION.toWarningCode();
        assertPlannerWarnings(queryRunner,
                "SELECT 1/2",
                Optional.of(warningCode));
        assertPlannerWarnings(queryRunner,
                "SELECT a/2 FROM (VALUES (1)) as t(a)",
                Optional.of(warningCode));
        assertPlannerWarnings(queryRunner,
                "SELECT 1/2.0",
                Optional.empty());
    }

    @Test
    public void testDeprecatedFunction()
    {
        WarningCode warningCode = StandardWarningCode.DREPRCATED_FUNCTION.toWarningCode();
        assertPlannerWarnings(queryRunner,
                "SELECT 123",
                Optional.empty());
        assertPlannerWarnings(queryRunner,
                "SELECT json_array_get('[1, 2, 3]', 0)",
                Optional.of(warningCode));
        assertPlannerWarnings(queryRunner,
                "SELECT json_array_get(a, 0) FROM (VALUES ('[1, 2, 3]')) AS t(a)",
                Optional.of(warningCode));
    }

    private static void assertPlannerWarnings(LocalQueryRunner queryRunner, @Language("SQL") String sql, Optional<WarningCode> expectedWarning)
    {
        assertPlannerWarnings(queryRunner, sql, ImmutableMap.of("enable_query_diagnosis_warning", "true"), expectedWarning);
    }

    private static void assertPlannerWarnings(LocalQueryRunner queryRunner, @Language("SQL") String sql, Map<String, String> sessionProperties, Optional<WarningCode> expectedWarning)
    {
        Session.SessionBuilder sessionBuilder = testSessionBuilder()
                .setCatalog(queryRunner.getDefaultSession().getCatalog().get())
                .setSchema(queryRunner.getDefaultSession().getSchema().get());
        sessionProperties.forEach(sessionBuilder::setSystemProperty);
        WarningCollector warningCollector = new DefaultWarningCollector(new WarningCollectorConfig());
        queryRunner.inTransaction(sessionBuilder.build(), transactionSession -> {
            queryRunner.createPlan(transactionSession, sql, LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED, warningCollector);
            return null;
        });
        Set<WarningCode> warnings = warningCollector.getWarnings().stream()
                .map(PrestoWarning::getWarningCode)
                .collect(toImmutableSet());
        if (expectedWarning.isPresent()) {
            if (!warnings.contains(expectedWarning.get())) {
                fail("Expected warning: " + expectedWarning);
            }
        }
        else {
            if (!warnings.isEmpty()) {
                fail("Expect no warning");
            }
        }
    }
}
