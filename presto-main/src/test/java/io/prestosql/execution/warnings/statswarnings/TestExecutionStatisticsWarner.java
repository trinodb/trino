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
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static io.prestosql.SystemSessionProperties.QUERY_CPU_AND_MEMORY_WARNING_THRESHOLD;
import static org.testng.Assert.assertTrue;

public class TestExecutionStatisticsWarner
{
    private QueryInfo queryInfo1;
    private QueryInfo queryInfo2;
    private Session session;

    @BeforeTest
    public void setup()
    {
        Session session = Session.builder(new SessionPropertyManager())
                .setQueryId(new QueryId("test_query_id"))
                .setIdentity(new Identity("testUser", Optional.empty()))
                .setSystemProperty(QUERY_CPU_AND_MEMORY_WARNING_THRESHOLD, "0.7")
                .setCatalogSessionProperty("testCatalog", "explicit_set", "explicit_set")
                .build();
        this.session = session;
        queryInfo1 = TestExecutionStatisticsWarnerUtil.getQueryInfo1();
        queryInfo2 = TestExecutionStatisticsWarnerUtil.getQueryInfo2();
    }

    private void assertWarning(List<PrestoWarning> warningList, int warningCode, String warningMessage)
    {
        assertTrue(warningList.stream().anyMatch(t -> t.getWarningCode().getCode() == warningCode));
        for (PrestoWarning warning : warningList) {
            if (warning.getWarningCode().getCode() == warningCode) {
                assertTrue(warning.getMessage().equals(warningMessage));
            }
        }
    }

    @Test
    public void testCombinedWarnings()
    {
        ExecutionStatisticsWarner statsWarner = new ExecutionStatisticsWarner(new MemoryManagerConfig(), new QueryManagerConfig());
        List<PrestoWarning> warningList = statsWarner.collectStatsWarnings(queryInfo1, session);
        assertWarning(warningList, 259, "Query Id 0's peakUserMemoryReservation has exceeded the threshold warning value. This query used 24GB of the userMemoryLimit of 20GB.");
        assertWarning(warningList, 261, "Query Id 0 has exceeded the max cpu warning threshold value. This query used 800000000.00d of the maxCPU of 1000000000.00d.");
        assertWarning(warningList, 262, "StageId: 1 has these skews: [Task Id: 1.1.1 CPU skew: 12.0],StageId: 2 has these skews: [Task Id: 1.1.1 CPU skew: 12.0]");
        assertWarning(warningList, 263, "Try swapping the join ordering for: (Tablename: [hive:table:sf1.0]) InnerJoin (Stage IDs: [2]) in StageId: 1");

        warningList = statsWarner.collectStatsWarnings(queryInfo2, session);
        assertWarning(warningList, 263, "Try swapping the join ordering for: (Stage IDs: [1]) InnerJoin (Stage IDs: [2]) in StageId: 1");
    }

    @Test
    public void testThresholdWarnings()
    {
        MemoryAndCPUThresholdWarningsGenerator thresholdWarningsGenerator =
                new MemoryAndCPUThresholdWarningsGenerator(
                        new Duration(20000000000.0, TimeUnit.DAYS),
                        new DataSize(200000.0, DataSize.Unit.GIGABYTE),
                        new DataSize(20.0, DataSize.Unit.GIGABYTE));

        List<PrestoWarning> warningList = thresholdWarningsGenerator.generateStatsWarnings(queryInfo1, session);
        assertWarning(warningList, 259, "Query Id 0's peakUserMemoryReservation has exceeded the threshold warning value. This query used 24GB of the userMemoryLimit of 20GB.");
        assertWarning(warningList, 260, "Query Id 0's peakTotalMemoryReservation has exceeded the threshold warning value. This query used 26GB of the totalMemoryLimit of 20GB");
        assertWarning(warningList, 261, "Query Id 0 has exceeded the max cpu warning threshold value. This query used 800000000.00d of the maxCPU of 1000000000.00d.");

        warningList = thresholdWarningsGenerator.generateStatsWarnings(queryInfo2, session);
        assertWarning(warningList, 260, "Query Id 1's peakTotalMemoryReservation has exceeded the threshold warning value. This query used 26GB of the totalMemoryLimit of 20GB");
    }

    @Test
    public void testSkewWarnings()
    {
        SkewWarningsGenerator skewWarningsGenerator = new SkewWarningsGenerator();

        List<PrestoWarning> warningList = skewWarningsGenerator.generateStatsWarnings(queryInfo1, session);
        assertWarning(warningList, 262, "StageId: 1 has these skews: [Task Id: 1.1.1 CPU skew: 12.0],StageId: 2 has these skews: [Task Id: 1.1.1 CPU skew: 12.0]");
        assertTrue(skewWarningsGenerator.generateStatsWarnings(queryInfo2, session).isEmpty());
    }

    @Test
    public void testJoinAdvice()
    {
        JoinWarningsGenerator joinWarningsGenerator = new JoinWarningsGenerator();

        List<PrestoWarning> warningList = joinWarningsGenerator.generateStatsWarnings(queryInfo1, session);
        assertWarning(warningList, 263, "Try swapping the join ordering for: (Tablename: [hive:table:sf1.0]) InnerJoin (Stage IDs: [2]) in StageId: 1");

        warningList = joinWarningsGenerator.generateStatsWarnings(queryInfo2, session);
        assertWarning(warningList, 263, "Try swapping the join ordering for: (Stage IDs: [1]) InnerJoin (Stage IDs: [2]) in StageId: 1");
    }
}
