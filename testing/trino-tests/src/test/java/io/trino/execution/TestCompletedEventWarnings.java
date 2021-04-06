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
import io.trino.Session.SessionBuilder;
import io.trino.execution.TestEventListenerPlugin.TestingEventListenerPlugin;
import io.trino.execution.events.EventCollectorConfig;
import io.trino.spi.TrinoEvent;
import io.trino.spi.TrinoWarning;
import io.trino.spi.WarningCode;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingEventCollector;
import io.trino.testing.TestingEventCollectorConfig;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestCompletedEventWarnings
{
    private static final int EXPECTED_EVENTS = 3;
    private static final int TEST_WARNINGS = 5;
    private static final int TEST_EVENTS = 7;
    private QueryRunner queryRunner;
    private EventsCollector generatedEvents;

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        SessionBuilder sessionBuilder = testSessionBuilder();
        generatedEvents = new EventsCollector();
        queryRunner = DistributedQueryRunner.builder(sessionBuilder.build())
                .setExtraProperties(ImmutableMap.of(
                        "testing-event-collector.preloaded-warnings", String.valueOf(TEST_WARNINGS),
                        "testing-event-collector.preloaded-events", String.valueOf(TEST_EVENTS)))
                .setNodeCount(1)
                .build();
        queryRunner.installPlugin(new TestingEventListenerPlugin(generatedEvents));
        generatedEvents.reset(EXPECTED_EVENTS);
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
    {
        queryRunner.close();
        queryRunner = null;
        generatedEvents = null;
    }

    @Test
    public void testCompletedEventWarnings()
            throws InterruptedException
    {
        TestingEventCollectorConfig eventCollectorConfig = new TestingEventCollectorConfig().setPreloadedWarnings(TEST_WARNINGS);
        TestingEventCollector testingEventCollector = new TestingEventCollector(new EventCollectorConfig(), eventCollectorConfig);
        assertWarnings(
                "select 1",
                ImmutableMap.of(),
                testingEventCollector.getWarnings().stream()
                        .map(TrinoWarning::getWarningCode)
                        .collect(toImmutableList()));
    }

    @Test
    public void testCompletedEvents()
            throws InterruptedException
    {
        TestingEventCollectorConfig eventCollectorConfig = new TestingEventCollectorConfig().setPreloadedEvents(TEST_EVENTS);
        TestingEventCollector testingEventCollector = new TestingEventCollector(new EventCollectorConfig(), eventCollectorConfig);
        assertEvents(
                "select 1",
                ImmutableMap.of(),
                testingEventCollector.getEvents().stream()
                        .map(TrinoEvent::getMessage)
                        .collect(toImmutableList()));
    }

    private void assertWarnings(@Language("SQL") String sql, Map<String, String> sessionProperties, List<WarningCode> expectedWarnings)
            throws InterruptedException
    {
        // Task concurrency must be 1 otherwise these tests fail due to change in the number of EXPECTED_EVENTS
        SessionBuilder sessionBuilder = testSessionBuilder()
                .setSystemProperty("task_concurrency", "1");
        sessionProperties.forEach(sessionBuilder::setSystemProperty);
        queryRunner.execute(sessionBuilder.build(), sql);
        generatedEvents.waitForEvents(10);

        Set<WarningCode> warnings = getOnlyElement(generatedEvents.getQueryCompletedEvents())
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

    private void assertEvents(@Language("SQL") String sql, Map<String, String> sessionProperties, List<String> expectedEvents)
            throws InterruptedException
    {
        // Task concurrency must be 1 otherwise these tests fail due to change in the number of EXPECTED_EVENTS
        SessionBuilder sessionBuilder = testSessionBuilder()
                .setSystemProperty("task_concurrency", "1");
        sessionProperties.forEach(sessionBuilder::setSystemProperty);
        queryRunner.execute(sessionBuilder.build(), sql);
        generatedEvents.waitForEvents(10);

        Set<String> events = getOnlyElement(generatedEvents.getQueryCompletedEvents())
                .getEvents()
                .stream()
                .map(TrinoEvent::getMessage)
                .collect(toImmutableSet());
        for (String expectedEvent : expectedEvents) {
            if (!events.contains(expectedEvent)) {
                fail("Expected warning: " + expectedEvent);
            }
        }
    }
}
