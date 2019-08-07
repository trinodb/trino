package io.prestosql.execution.warnings.statswarnings;

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

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.Session;
import io.prestosql.execution.QueryInfo;
import io.prestosql.execution.QueryManagerConfig;
import io.prestosql.memory.MemoryManagerConfig;
import io.prestosql.metadata.SessionPropertyManager;
import io.prestosql.spi.PrestoWarning;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.security.Identity;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static io.prestosql.SystemSessionProperties.QUERY_CPU_AND_MEMORY_WARNING_THRESHOLD;
import static io.prestosql.SystemSessionProperties.QUERY_MAX_CPU_TIME;
import static io.prestosql.spi.connector.StandardWarningCode.STAGE_SKEW;
import static io.prestosql.spi.connector.StandardWarningCode.TOTAL_CPU_TIME_OVER_THRESHOLD_VAL;
import static io.prestosql.spi.connector.StandardWarningCode.TOTAL_MEMORY_LIMIT_OVER_THRESHOLD_VAL;
import static io.prestosql.spi.connector.StandardWarningCode.USER_MEMORY_LIMIT_OVER_THRESHOLD_VAL;
import static org.testng.Assert.assertTrue;

public class TestExecutionStatisticsWarner
{
    private QueryInfo queryInfo1;
    private QueryInfo queryInfo2;
    private Session session;

    @BeforeClass
    public void setup() throws Exception
    {
        Session session = Session.builder(new SessionPropertyManager())
                .setQueryId(new QueryId("test_query_id"))
                .setIdentity(new Identity("testUser", Optional.empty()))
                .setSystemProperty(QUERY_CPU_AND_MEMORY_WARNING_THRESHOLD, "0.7")
                .setSystemProperty(QUERY_MAX_CPU_TIME, "1d")
                .setCatalogSessionProperty("testCatalog", "explicit_set", "explicit_set")
                .build();
        this.session = session;
        queryInfo1 = TestExecutionStatisticsWarnerUtil.getQueryInfo1();
        queryInfo2 = TestExecutionStatisticsWarnerUtil.getQueryInfo2();
    }

    private void assertWarning(List<PrestoWarning> warningList, List<PrestoWarning> expectedWarnings)
    {
        assertTrue(warningList.size() == expectedWarnings.size());
        for (PrestoWarning warning : warningList) {
            assertTrue(expectedWarnings.contains(warning));
        }
    }

    @Test
    public void testCombinedWarnings()
    {
        ExecutionStatisticsWarner statsWarner = new ExecutionStatisticsWarner(new MemoryManagerConfig(), new QueryManagerConfig());
        List<PrestoWarning> warningList = statsWarner.collectExecutionStatisticsWarnings(queryInfo1, session);
        List<PrestoWarning> expectedWarnings = ImmutableList.of(
                new PrestoWarning(USER_MEMORY_LIMIT_OVER_THRESHOLD_VAL, "Query Id 0's peak user memory reached 17179869184B of the maximum allowed value of 21474836480B."),
                new PrestoWarning(TOTAL_CPU_TIME_OVER_THRESHOLD_VAL, "Query Id 0 has exceeded the max cpu warning threshold value. This query used 79200000.00ms of the maxCPU of 86400000.00ms."),
                new PrestoWarning(STAGE_SKEW, "StageId 1 has these tasks' CPU times take long than others [Task Id 1.1]. Mean 2.833, Median 1.000"),
                new PrestoWarning(STAGE_SKEW, "StageId 2 has these tasks' CPU times take long than others [Task Id 2.1]. Mean 2.833, Median 1.000"));

        assertWarning(warningList, expectedWarnings);

        warningList = statsWarner.collectExecutionStatisticsWarnings(queryInfo2, session);
        assertWarning(warningList, ImmutableList.of(
                new PrestoWarning(TOTAL_CPU_TIME_OVER_THRESHOLD_VAL, "Query Id 0 has exceeded the max cpu warning threshold value. This query used 79200000.00ms of the maxCPU of 86400000.00ms.")));
    }

    @Test
    public void testThresholdWarnings()
    {
        CPUAndMemoryThresholdWarningsGenerator thresholdWarningsGenerator =
                new CPUAndMemoryThresholdWarningsGenerator(
                        new Duration(30.0, TimeUnit.DAYS),
                        new DataSize(20.0, DataSize.Unit.GIGABYTE),
                        new DataSize(20.0, DataSize.Unit.GIGABYTE));

        List<PrestoWarning> warningList = thresholdWarningsGenerator.generateExecutionStatisticsWarnings(queryInfo1, session);
        List<PrestoWarning> expectedWarnings = ImmutableList.of(
                new PrestoWarning(USER_MEMORY_LIMIT_OVER_THRESHOLD_VAL, "Query Id 0's peak user memory reached 17179869184B of the maximum allowed value of 21474836480B."),
                new PrestoWarning(TOTAL_MEMORY_LIMIT_OVER_THRESHOLD_VAL, "Query Id 0's peak total memory reached 17179869184B of the maximum allowed value of 21474836480B"),
                new PrestoWarning(TOTAL_CPU_TIME_OVER_THRESHOLD_VAL, "Query Id 0 has exceeded the max cpu warning threshold value. This query used 79200000.00ms of the maxCPU of 86400000.00ms."));
        assertWarning(warningList, expectedWarnings);

        warningList = thresholdWarningsGenerator.generateExecutionStatisticsWarnings(queryInfo2, session);
        assertWarning(warningList, ImmutableList.of(
                new PrestoWarning(TOTAL_CPU_TIME_OVER_THRESHOLD_VAL, "Query Id 0 has exceeded the max cpu warning threshold value. This query used 79200000.00ms of the maxCPU of 86400000.00ms.")));
    }

    @Test
    public void testSkewWarnings()
    {
        SkewWarningsGenerator skewWarningsGenerator = new SkewWarningsGenerator();

        List<PrestoWarning> warningList = skewWarningsGenerator.generateExecutionStatisticsWarnings(queryInfo1, session);
        List<PrestoWarning> expectedWarnings = ImmutableList.of(
                new PrestoWarning(STAGE_SKEW, "StageId 1 has these tasks' CPU times take long than others [Task Id 1.1]. Mean 2.833, Median 1.000"),
                new PrestoWarning(STAGE_SKEW, "StageId 2 has these tasks' CPU times take long than others [Task Id 2.1]. Mean 2.833, Median 1.000"));
        assertWarning(warningList, expectedWarnings);
        assertTrue(skewWarningsGenerator.generateExecutionStatisticsWarnings(queryInfo2, session).isEmpty());
    }
}
