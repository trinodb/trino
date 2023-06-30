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
package io.trino.plugin.jdbc;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.testing.TestingConnectorContext;
import org.testng.annotations.Test;

import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import java.util.Set;

import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.String.format;
import static java.lang.management.ManagementFactory.getPlatformMBeanServer;
import static org.assertj.core.api.Assertions.assertThat;

public class TestJmxStats
{
    @Test
    public void testJmxStatsExposure()
            throws Exception
    {
        Plugin plugin = new JdbcPlugin("base_jdbc", new TestingH2JdbcModule());
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());
        factory.create("test", ImmutableMap.of("connection-url", "jdbc:driver:"), new TestingConnectorContext());
        MBeanServer mbeanServer = getPlatformMBeanServer();
        Set<ObjectName> objectNames = mbeanServer.queryNames(new ObjectName("io.trino.plugin.jdbc:*"), null);

        assertThat(objectNames.containsAll(
                ImmutableSet.of(
                        new ObjectName("io.trino.plugin.jdbc:type=ConnectionFactory,name=test"),
                        new ObjectName("io.trino.plugin.jdbc:type=JdbcClient,name=test")))).isTrue();

        for (ObjectName objectName : objectNames) {
            MBeanInfo mbeanInfo = mbeanServer.getMBeanInfo(objectName);
            assertThat(mbeanInfo.getAttributes().length).withFailMessage(format("Object %s doesn't expose JMX stats", objectName.getCanonicalName())).isNotEqualTo(0);
        }
    }
}
