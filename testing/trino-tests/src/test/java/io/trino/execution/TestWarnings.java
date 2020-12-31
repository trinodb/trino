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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.Session;
import io.prestosql.client.Warning;
import io.prestosql.spi.WarningCode;
import io.prestosql.testing.QueryRunner;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.prestosql.execution.TestQueryRunnerUtil.createQueryRunner;
import static io.prestosql.spi.connector.StandardWarningCode.TOO_MANY_STAGES;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.fail;

public class TestWarnings
{
    private static final int STAGE_COUNT_WARNING_THRESHOLD = 20;
    private QueryRunner queryRunner;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        queryRunner = createQueryRunner(ImmutableMap.of("query.stage-count-warning-threshold", String.valueOf(STAGE_COUNT_WARNING_THRESHOLD)));
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        queryRunner.close();
        queryRunner = null;
    }

    @Test
    public void testStageCountWarningThreshold()
    {
        StringBuilder queryBuilder = new StringBuilder("SELECT name FROM nation WHERE nationkey = 0");
        String noWarningsQuery = queryBuilder.toString();
        for (int stageIndex = 1; stageIndex <= STAGE_COUNT_WARNING_THRESHOLD; stageIndex++) {
            queryBuilder.append("  UNION")
                    .append("  SELECT name FROM nation WHERE nationkey = ")
                    .append(stageIndex);
        }
        String query = queryBuilder.toString();
        Session session = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("tiny")
                .build();
        assertWarnings(queryRunner, session, query, ImmutableList.of(TOO_MANY_STAGES.toWarningCode()));
        assertWarnings(queryRunner, session, noWarningsQuery, ImmutableList.of());
    }

    private static void assertWarnings(QueryRunner queryRunner, Session session, @Language("SQL") String sql, List<WarningCode> expectedWarnings)
    {
        Set<Integer> warnings = queryRunner.execute(session, sql).getWarnings().stream()
                .map(Warning::getWarningCode)
                .map(Warning.Code::getCode)
                .collect(toImmutableSet());

        for (WarningCode warningCode : expectedWarnings) {
            if (!warnings.contains(warningCode.getCode())) {
                fail("Expected warning: " + warningCode);
            }
        }
    }
}
