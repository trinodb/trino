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
package io.trino.execution.scheduler;

import com.google.common.io.Files;
import com.google.common.io.Resources;
import io.airlift.testing.TempFile;
import io.airlift.testing.TestingTicker;
import io.airlift.units.Duration;
import io.trino.spi.HostAddress;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.DAYS;
import static org.testng.Assert.assertEquals;

public class TestFileBasedNetworkTopology
{
    private File topologyFile;
    private File topologyNewFile;

    @BeforeClass
    public void setup()
            throws Exception
    {
        topologyFile = new File(Resources.getResource(getClass(), "topology.txt").toURI());
        topologyNewFile = new File(Resources.getResource(getClass(), "topology-new.txt").toURI());
    }

    @Test
    public void testLocate()
    {
        NetworkTopology topology = new FileBasedNetworkTopology(topologyFile, new Duration(1, DAYS), new TestingTicker());

        assertEquals(topology.locate(HostAddress.fromString("0.0.0.0")), new NetworkLocation());
        assertEquals(topology.locate(HostAddress.fromString("not-exist.example.com")), new NetworkLocation());

        assertEquals(topology.locate(HostAddress.fromString("192.168.0.1")), new NetworkLocation("region1", "rack1", "machine1"));
        assertEquals(topology.locate(HostAddress.fromString("192.168.0.2")), new NetworkLocation("region1", "rack1", "machine2"));
        assertEquals(topology.locate(HostAddress.fromString("hdfs01.example.com")), new NetworkLocation("region2", "rack2", "machine3"));

        assertEquals(topology.locate(HostAddress.fromString("192.168.0.1:8080")), new NetworkLocation("region1", "rack1", "machine1"));
    }

    @Test
    public void testRefresh()
            throws Exception
    {
        try (TempFile tempFile = new TempFile()) {
            Files.copy(topologyFile, tempFile.file());

            TestingTicker ticker = new TestingTicker();
            FileBasedNetworkTopology topology = new FileBasedNetworkTopology(tempFile.file(), new Duration(1, DAYS), ticker);

            assertEquals(topology.locate(HostAddress.fromString("not-exist.example.com")), new NetworkLocation());
            assertEquals(topology.locate(HostAddress.fromString("192.168.0.1")), new NetworkLocation("region1", "rack1", "machine1"));
            assertEquals(topology.locate(HostAddress.fromString("192.168.0.2")), new NetworkLocation("region1", "rack1", "machine2"));
            assertEquals(topology.locate(HostAddress.fromString("192.168.0.3")), new NetworkLocation());

            assertEquals(topology.locate(HostAddress.fromString("new")), new NetworkLocation());
            Files.copy(topologyNewFile, tempFile.file());
            ticker.increment(1, TimeUnit.DAYS);

            assertEquals(topology.locate(HostAddress.fromString("new")), new NetworkLocation("new", "rack", "machine"));
            assertEquals(topology.locate(HostAddress.fromString("not-exist.example.com")), new NetworkLocation());
            assertEquals(topology.locate(HostAddress.fromString("192.168.0.1")), new NetworkLocation("region1", "rack1", "machine5"));
            assertEquals(topology.locate(HostAddress.fromString("192.168.0.2")), new NetworkLocation());
            assertEquals(topology.locate(HostAddress.fromString("192.168.0.3")), new NetworkLocation("region1", "rack1", "machine6"));
        }
    }
}
