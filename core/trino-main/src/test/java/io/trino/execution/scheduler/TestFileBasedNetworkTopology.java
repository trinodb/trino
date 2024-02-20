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
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.URISyntaxException;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.DAYS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestFileBasedNetworkTopology
{
    private final File topologyFile;
    private final File topologyNewFile;

    public TestFileBasedNetworkTopology()
            throws URISyntaxException
    {
        topologyFile = new File(Resources.getResource(getClass(), "topology.txt").toURI());
        topologyNewFile = new File(Resources.getResource(getClass(), "topology-new.txt").toURI());
    }

    @Test
    public void testLocate()
    {
        NetworkTopology topology = new FileBasedNetworkTopology(topologyFile, new Duration(1, DAYS), new TestingTicker());

        assertThat(topology.locate(HostAddress.fromString("0.0.0.0"))).isEqualTo(new NetworkLocation());
        assertThat(topology.locate(HostAddress.fromString("not-exist.example.com"))).isEqualTo(new NetworkLocation());

        assertThat(topology.locate(HostAddress.fromString("192.168.0.1"))).isEqualTo(new NetworkLocation("region1", "rack1", "machine1"));
        assertThat(topology.locate(HostAddress.fromString("192.168.0.2"))).isEqualTo(new NetworkLocation("region1", "rack1", "machine2"));
        assertThat(topology.locate(HostAddress.fromString("hdfs01.example.com"))).isEqualTo(new NetworkLocation("region2", "rack2", "machine3"));

        assertThat(topology.locate(HostAddress.fromString("192.168.0.1:8080"))).isEqualTo(new NetworkLocation("region1", "rack1", "machine1"));
    }

    @Test
    public void testRefresh()
            throws Exception
    {
        try (TempFile tempFile = new TempFile()) {
            Files.copy(topologyFile, tempFile.file());

            TestingTicker ticker = new TestingTicker();
            FileBasedNetworkTopology topology = new FileBasedNetworkTopology(tempFile.file(), new Duration(1, DAYS), ticker);

            assertThat(topology.locate(HostAddress.fromString("not-exist.example.com"))).isEqualTo(new NetworkLocation());
            assertThat(topology.locate(HostAddress.fromString("192.168.0.1"))).isEqualTo(new NetworkLocation("region1", "rack1", "machine1"));
            assertThat(topology.locate(HostAddress.fromString("192.168.0.2"))).isEqualTo(new NetworkLocation("region1", "rack1", "machine2"));
            assertThat(topology.locate(HostAddress.fromString("192.168.0.3"))).isEqualTo(new NetworkLocation());

            assertThat(topology.locate(HostAddress.fromString("new"))).isEqualTo(new NetworkLocation());
            Files.copy(topologyNewFile, tempFile.file());
            ticker.increment(1, TimeUnit.DAYS);

            assertThat(topology.locate(HostAddress.fromString("new"))).isEqualTo(new NetworkLocation("new", "rack", "machine"));
            assertThat(topology.locate(HostAddress.fromString("not-exist.example.com"))).isEqualTo(new NetworkLocation());
            assertThat(topology.locate(HostAddress.fromString("192.168.0.1"))).isEqualTo(new NetworkLocation("region1", "rack1", "machine5"));
            assertThat(topology.locate(HostAddress.fromString("192.168.0.2"))).isEqualTo(new NetworkLocation());
            assertThat(topology.locate(HostAddress.fromString("192.168.0.3"))).isEqualTo(new NetworkLocation("region1", "rack1", "machine6"));
        }
    }
}
