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
package io.presto.eventlog;

import io.presto.eventlog.conf.EventLogConfig;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestEventLogProcessor
{
    private EventLogProcessor eventLogProcessor;

    @BeforeClass
    public void setUp()
    {
        EventLogConfig config = new EventLogConfig();
        config.setEnableEventLog(true);
        config.setEventLogDir("file:///tmp/presto-event/test");
        eventLogProcessor = new EventLogProcessor(config);
    }

    @Test
    public void testWriteEventLog()
    {
        eventLogProcessor.writeEventLog("{\"queryId\":\"20200408_064646_00000_kqes4\"}", "20200408_064646_00000_kqes4");
        File file = new File("/tmp/presto-event/test/20200408_064646_00000_kqes4");
        assertTrue(file.exists());
    }

    @Test
    public void testReadQueryInfo()
    {
        eventLogProcessor.writeEventLog("{\"queryId\":\"20200408_064646_00000_kqes4\"}", "20200408_064646_00000_kqes4");
        try {
            String queryInfo = eventLogProcessor.readQueryInfo("20200408_064646_00000_kqes4");
            assertNotNull(queryInfo);
        }
        catch (IOException e) {
            fail(e.getMessage());
        }
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        File file = new File("/tmp/presto-event/test/20200408_064646_00000_kqes4");
        if (file.exists()) {
            file.delete();
        }
    }
}
