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

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closer;
import io.trino.execution.EventsCollector.QueryEvents;
import io.trino.execution.TestEventListenerPlugin.TestingEventListenerPlugin;
import io.trino.execution.warnings.WarningCollectorConfig;
import io.trino.spi.TrinoWarning;
import io.trino.spi.WarningCode;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.TestingWarningCollector;
import io.trino.testing.TestingWarningCollectorConfig;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestCompletedEventWarnings
{
    private static final int TEST_WARNINGS = 5;

    private final EventsCollector generatedEvents = new EventsCollector();

    private Closer closer;
    private EventsAwaitingQueries queries;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        closer = Closer.create();
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(TEST_SESSION)
                .setExtraProperties(ImmutableMap.of("testing-warning-collector.preloaded-warnings", String.valueOf(TEST_WARNINGS)))
                .setNodeCount(1)
                .build();
        closer.register(queryRunner);
        queryRunner.installPlugin(new TestingEventListenerPlugin(generatedEvents));
        queries = new EventsAwaitingQueries(generatedEvents, queryRunner);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws IOException
    {
        if (closer != null) {
            closer.close();
        }
        closer = null;
    }

    @Test
    public void testCompletedEventWarnings()
            throws Exception
    {
        TestingWarningCollectorConfig warningCollectorConfig = new TestingWarningCollectorConfig().setPreloadedWarnings(TEST_WARNINGS);
        TestingWarningCollector testingWarningCollector = new TestingWarningCollector(new WarningCollectorConfig(), warningCollectorConfig);
        assertWarnings(
                "select 1",
                testingWarningCollector.getWarnings().stream()
                        .map(TrinoWarning::getWarningCode)
                        .collect(toImmutableList()));
    }

    private void assertWarnings(@Language("SQL") String sql, List<WarningCode> expectedWarnings)
            throws Exception
    {
        QueryEvents queryEvents = queries.runQueryAndWaitForEvents(sql, TEST_SESSION).getQueryEvents();

        Set<WarningCode> warnings = queryEvents.getQueryCompletedEvent()
                .getWarnings()
                .stream()
                .map(TrinoWarning::getWarningCode)
                .collect(toImmutableSet());
        for (WarningCode warningCode : expectedWarnings) {
            if (!warnings.contains(warningCode)) {
                fail("Expected warning: " + warningCode);
            }
        }
    }
}
