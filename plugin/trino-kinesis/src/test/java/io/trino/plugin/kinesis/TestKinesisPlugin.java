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
package io.trino.plugin.kinesis;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.kinesis.util.TestUtils;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.testing.TestingConnectorContext;
import org.testng.annotations.Test;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.spi.transaction.IsolationLevel.READ_COMMITTED;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * Test that the plug in API is satisfied and all of the required objects can be created.
 * <p>
 * This will not make any calls to AWS, it merely checks that all of the Plug in SPI
 * objects are in place.
 */
public class TestKinesisPlugin
{
    @Test
    public void testCreateConnector()
    {
        KinesisPlugin plugin = new KinesisPlugin();
        ConnectorFactory factory = getOnlyElement(plugin.getConnectorFactories());

        String accessKey = "kinesis.accessKey";
        String secretKey = "kinesis.secretKey";

        Connector c = factory.create("kinesis.test-connector", ImmutableMap.<String, String>builder()
                .put("kinesis.hide-internal-columns", "false")
                .put("kinesis.access-key", TestUtils.noneToBlank(accessKey))
                .put("kinesis.secret-key", TestUtils.noneToBlank(secretKey))
                .buildOrThrow(), new TestingConnectorContext());
        assertNotNull(c);

        // Verify that the key objects have been created on the connector
        assertNotNull(c.getRecordSetProvider());
        assertNotNull(c.getSplitManager());
        ConnectorMetadata md = c.getMetadata(SESSION, KinesisTransactionHandle.INSTANCE);
        assertNotNull(md);

        ConnectorTransactionHandle handle = c.beginTransaction(READ_COMMITTED, true, true);
        assertTrue(handle instanceof KinesisTransactionHandle);

        c.shutdown();
    }
}
