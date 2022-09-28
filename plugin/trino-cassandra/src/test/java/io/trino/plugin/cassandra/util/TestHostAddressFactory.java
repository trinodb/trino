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
package io.trino.plugin.cassandra.util;

import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.session.ProgrammaticArguments;
import com.datastax.oss.driver.internal.core.context.DefaultDriverContext;
import com.datastax.oss.driver.internal.core.context.InternalDriverContext;
import com.datastax.oss.driver.internal.core.metadata.DefaultEndPoint;
import com.datastax.oss.driver.internal.core.metadata.DefaultNode;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.HostAddress;
import org.testng.annotations.Test;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Set;

import static org.testng.Assert.assertEquals;

public class TestHostAddressFactory
{
    @Test
    public void testToHostAddressList()
            throws Exception
    {
        DriverConfigLoader driverConfigLoader = DriverConfigLoader.programmaticBuilder().build();
        ProgrammaticArguments args =
                ProgrammaticArguments.builder().build();
        InternalDriverContext driverContext = new DefaultDriverContext(driverConfigLoader, args);

        Set<Node> nodes = ImmutableSet.of(
                new DefaultNode(
                        new DefaultEndPoint(
                                new InetSocketAddress(
                                        InetAddress.getByAddress(new byte[] {
                                                1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16
                                        }),
                                        3000)),
                        driverContext),
                new DefaultNode(
                        new DefaultEndPoint(new InetSocketAddress(InetAddress.getByAddress(new byte[] {1, 2, 3, 4}), 3000)),
                        driverContext));

        HostAddressFactory hostAddressFactory = new HostAddressFactory();
        List<HostAddress> list = hostAddressFactory.toHostAddressList(nodes);

        assertEquals(list.toString(), "[[102:304:506:708:90a:b0c:d0e:f10], 1.2.3.4]");
    }
}
