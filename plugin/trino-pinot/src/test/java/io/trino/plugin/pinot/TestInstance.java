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
package io.trino.plugin.pinot;

import org.apache.helix.model.InstanceConfig;
import org.apache.pinot.core.transport.ServerInstance;
import org.junit.jupiter.api.Test;

import static org.testng.Assert.assertEquals;

public class TestInstance
{
    @Test
    public void test()
    {
        InstanceConfig instanceConfig = new InstanceConfig("Server_pinot-server-0.svc.cluster.local_9000");
        ServerInstance serverInstance = new ServerInstance(instanceConfig);
        assertEquals(serverInstance.getHostname(), "pinot-server-0.svc.cluster.local");
        assertEquals(serverInstance.getPort(), 9000);
    }
}
