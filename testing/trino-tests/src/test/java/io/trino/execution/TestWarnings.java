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
package io.trino.execution;

import com.google.common.collect.ImmutableList;
import io.trino.client.Warning;
import io.trino.spi.WarningCode;
import io.trino.testing.QueryRunner;
import io.trino.tests.tpch.TpchQueryRunnerBuilder;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.spi.connector.StandardWarningCode.TOO_MANY_STAGES;
import static org.testng.Assert.fail;

public class TestWarnings
{
    private static final int STAGE_COUNT_WARNING_THRESHOLD = 20;
    private QueryRunner queryRunner;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        queryRunner = TpchQueryRunnerBuilder.builder()
                .addExtraProperty("query.stage-count-warning-threshold", String.valueOf(STAGE_COUNT_WARNING_THRESHOLD))
                .build();
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
        assertWarnings(queryRunner, query, ImmutableList.of(TOO_MANY_STAGES.toWarningCode()));
        assertWarnings(queryRunner, noWarningsQuery, ImmutableList.of());
    }

    private static void assertWarnings(QueryRunner queryRunner, @Language("SQL") String sql, List<WarningCode> expectedWarnings)
    {
        Set<Integer> warnings = queryRunner.execute(sql).getWarnings().stream()
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
