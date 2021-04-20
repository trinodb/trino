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
package io.trino.execution.events;

import com.google.common.collect.ImmutableList;
import io.trino.spi.TrinoWarning;
import io.trino.testing.TestingEventCollector;
import io.trino.testing.TestingEventCollectorConfig;
import org.testng.annotations.Test;

import static io.trino.testing.TestingEventCollector.createTestWarning;
import static org.testng.Assert.assertEquals;

public class TestTestingEventCollector
{
    @Test
    public void testAddWarnings()
    {
        TestingEventCollector collector = new TestingEventCollector(new EventCollectorConfig(), new TestingEventCollectorConfig().setAddWarnings(true));
        ImmutableList.Builder<TrinoWarning> expectedWarningsBuilder = ImmutableList.builder();
        expectedWarningsBuilder.add(createTestWarning(1));
        assertEquals(collector.getWarnings(), expectedWarningsBuilder.build());
    }
}
