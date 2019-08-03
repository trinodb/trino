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
package io.prestosql.execution.scheduler;

import io.airlift.units.Duration;
import io.prestosql.spi.HostAddress;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Collections;

import static java.lang.Thread.sleep;
import static org.testng.Assert.assertEquals;

public class TestFileBasedNetworkTopology
{
    private FileBasedNetworkTopology topology;

    @BeforeTest
    public void setup()
    {
        String topologyFile = getClass().getResource("topology.txt").getFile();
        TopologyAwareNodeSelectorConfig config = new TopologyAwareNodeSelectorConfig()
                .setLocationSegmentNames("region,rack,machine")
                .setNetworkTopologyFile(topologyFile);
        topology = new FileBasedNetworkTopology(config);
    }

    @Test
    public void testLocate()
    {
        assertEquals(topology.locate(HostAddress.fromString("0.0.0.0")).getSegments(), Collections.emptyList());
        assertEquals(topology.locate(HostAddress.fromString("not-exist.example.com")).getSegments(), Collections.emptyList());

        assertEquals(topology.locate(HostAddress.fromString("192.168.0.1")).getSegments(), Arrays.asList("region1", "rack1", "machine1"));
        assertEquals(topology.locate(HostAddress.fromString("192.168.0.2")).getSegments(), Arrays.asList("region1", "rack1", "machine2"));
        assertEquals(topology.locate(HostAddress.fromString("hdfs01.example.com")).getSegments(), Arrays.asList("region2", "rack2", "machine3"));

        assertEquals(topology.locate(HostAddress.fromString("192.168.0.1:8080")).getSegments(), Arrays.asList("region1", "rack1", "machine1"));
    }

    @Test
    public void testReflash() throws Exception
    {
        Path tempFile = Files.createTempFile("test-topology", ".txt");
        tempFile.toFile().deleteOnExit();
        Files.copy(Paths.get(getClass().getResource("topology.txt").toURI()), tempFile, StandardCopyOption.REPLACE_EXISTING);

        TopologyAwareNodeSelectorConfig config = new TopologyAwareNodeSelectorConfig()
                .setNetworkTopologyFile(tempFile.toString())
                .setRefreshPeriod(Duration.valueOf("1s"));
        topology = new FileBasedNetworkTopology(config);

        assertEquals(topology.locate(HostAddress.fromString("not-exist.example.com")).getSegments(), Collections.emptyList());
        assertEquals(topology.locate(HostAddress.fromString("192.168.0.1")).getSegments(), Arrays.asList("region1", "rack1", "machine1"));
        assertEquals(topology.locate(HostAddress.fromString("192.168.0.2")).getSegments(), Arrays.asList("region1", "rack1", "machine2"));
        assertEquals(topology.locate(HostAddress.fromString("192.168.0.3")).getSegments(), Collections.emptyList());

        Files.copy(Paths.get(getClass().getResource("topology-new.txt").toURI()), tempFile, StandardCopyOption.REPLACE_EXISTING);
        sleep(1500);

        assertEquals(topology.locate(HostAddress.fromString("not-exist.example.com")).getSegments(), Collections.emptyList());
        assertEquals(topology.locate(HostAddress.fromString("192.168.0.1")).getSegments(), Arrays.asList("region1", "rack1", "machine5"));
        assertEquals(topology.locate(HostAddress.fromString("192.168.0.2")).getSegments(), Collections.emptyList());
        assertEquals(topology.locate(HostAddress.fromString("192.168.0.3")).getSegments(), Arrays.asList("region1", "rack1", "machine6"));
    }
}
