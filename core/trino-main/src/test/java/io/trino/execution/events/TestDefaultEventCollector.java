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

import io.trino.spi.TrinoWarning;
import io.trino.spi.WarningCode;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestDefaultEventCollector
{
    @Test
    public void testNoWarnings()
    {
        EventCollector eventCollector = new DefaultEventCollector(new EventCollectorConfig().setMaxWarnings(0));
        eventCollector.add(new TrinoWarning(new WarningCode(1, "1"), "warning 1"));
        assertEquals(eventCollector.getWarnings().size(), 0);
    }

    @Test
    public void testMaxWarnings()
    {
        EventCollector eventCollector = new DefaultEventCollector(new EventCollectorConfig().setMaxWarnings(2));
        eventCollector.add(new TrinoWarning(new WarningCode(1, "1"), "warning 1"));
        eventCollector.add(new TrinoWarning(new WarningCode(2, "2"), "warning 2"));
        eventCollector.add(new TrinoWarning(new WarningCode(3, "3"), "warning 3"));
        assertEquals(eventCollector.getWarnings().size(), 2);
    }
}
