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
package io.prestosql.event;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.execution.QueryInfo;
import io.prestosql.execution.QueryState;
import io.prestosql.execution.QueryStats;
import io.prestosql.server.testing.TestingPrestoServer;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.memory.MemoryPoolId;
import org.joda.time.DateTime;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Optional;

import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.execution.QueryState.QUEUED;
import static io.prestosql.operator.BlockedReason.WAITING_FOR_MEMORY;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestEventLogProcessor
{
    private TestingPrestoServer server;
    private EventLogProcessor eventLogProcessor;
    private QueryInfo queryInfo;

    @BeforeMethod
    public void setUp()
    {
        queryInfo = createQueryInfo("query_root_a_x", QUEUED, "SELECT 1");
        server = TestingPrestoServer.create();
        server = TestingPrestoServer.builder()
                .setProperties(ImmutableMap.<String, String>builder()
                        .put("event-log.enabled", "true")
                        .put("event-log.dir", "file:///tmp/presto-events")
                        .build())
                .build();
        eventLogProcessor = server.getEventLogProcessor();
    }

    @Test
    public void testWriteEventLog()
    {
        eventLogProcessor.writeEventLog(queryInfo);
        File file = new File("/tmp/presto-events/" + queryInfo.getQueryId().getId());
        assertTrue(file.exists());
    }

    @Test
    public void testReadQueryInfo()
    {
        try {
            eventLogProcessor.writeEventLog(queryInfo);
            QueryInfo queryInfoFromFile = eventLogProcessor.readQueryInfo(queryInfo.getQueryId().getId());
            assertNotNull(queryInfoFromFile);
        }
        catch (IOException e) {
            e.printStackTrace();
            fail();
        }
    }

    @AfterMethod
    public void tearDown()
    {
        String id = queryInfo.getQueryId().getId();
        File file = new File("/tmp/presto-events/" + id);
        if (file.exists()) {
            file.delete();
        }
    }

    private QueryInfo createQueryInfo(String queryId, QueryState state, String query)
    {
        return new QueryInfo(
                new QueryId(queryId),
                TEST_SESSION.toSessionRepresentation(),
                state,
                new MemoryPoolId("reserved"),
                true,
                URI.create("1"),
                ImmutableList.of("2", "3"),
                query,
                Optional.empty(),
                new QueryStats(
                        DateTime.parse("1991-09-06T05:00-05:30"),
                        DateTime.parse("1991-09-06T05:01-05:30"),
                        DateTime.parse("1991-09-06T05:02-05:30"),
                        DateTime.parse("1991-09-06T06:00-05:30"),
                        Duration.valueOf("10s"),
                        Duration.valueOf("8m"),
                        Duration.valueOf("7m"),
                        Duration.valueOf("34m"),
                        Duration.valueOf("9m"),
                        Duration.valueOf("10m"),
                        Duration.valueOf("11m"),
                        Duration.valueOf("12m"),
                        13,
                        14,
                        15,
                        100,
                        17,
                        18,
                        34,
                        19,
                        20.0,
                        DataSize.valueOf("21GB"),
                        DataSize.valueOf("22GB"),
                        DataSize.valueOf("23GB"),
                        DataSize.valueOf("24GB"),
                        DataSize.valueOf("25GB"),
                        DataSize.valueOf("26GB"),
                        DataSize.valueOf("27GB"),
                        DataSize.valueOf("28GB"),
                        DataSize.valueOf("29GB"),
                        true,
                        Duration.valueOf("23m"),
                        Duration.valueOf("24m"),
                        Duration.valueOf("26m"),
                        true,
                        ImmutableSet.of(WAITING_FOR_MEMORY),
                        DataSize.valueOf("271GB"),
                        281,
                        Duration.valueOf("26m"),
                        DataSize.valueOf("272GB"),
                        282,
                        DataSize.valueOf("27GB"),
                        28,
                        DataSize.valueOf("29GB"),
                        30,
                        DataSize.valueOf("31GB"),
                        32,
                        DataSize.valueOf("33GB"),
                        ImmutableList.of(),
                        ImmutableList.of()),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of(),
                ImmutableSet.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableSet.of(),
                Optional.empty(),
                false,
                "33",
                Optional.empty(),
                null,
                null,
                ImmutableList.of(),
                ImmutableSet.of(),
                Optional.empty(),
                false,
                Optional.empty());
    }
}
