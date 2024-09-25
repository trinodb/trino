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
package io.trino.plugin.pulsar;

import org.apache.pulsar.common.policies.data.OffloadPolicies;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestPulsarConnectorConfig
{
    @Test
    public void testDefaultNamespaceDelimiterRewrite()
    {
        PulsarConnectorConfig connectorConfig = new PulsarConnectorConfig();
        assertFalse(connectorConfig.getNamespaceDelimiterRewriteEnable());
        assertEquals("/", connectorConfig.getRewriteNamespaceDelimiter());
    }

    @Test
    public void testNamespaceRewriteDelimiterRestriction()
    {
        PulsarConnectorConfig connectorConfig = new PulsarConnectorConfig();
        try {
            connectorConfig.setRewriteNamespaceDelimiter("-=:.Az09_");
        }
        catch (Exception e) {
            assertTrue(e instanceof IllegalArgumentException);
        }
        connectorConfig.setRewriteNamespaceDelimiter("|");
        assertEquals("|", (connectorConfig.getRewriteNamespaceDelimiter()));
        connectorConfig.setRewriteNamespaceDelimiter("||");
        assertEquals("||", (connectorConfig.getRewriteNamespaceDelimiter()));
        connectorConfig.setRewriteNamespaceDelimiter("$");
        assertEquals("$", (connectorConfig.getRewriteNamespaceDelimiter()));
        connectorConfig.setRewriteNamespaceDelimiter("&");
        assertEquals("&", (connectorConfig.getRewriteNamespaceDelimiter()));
        connectorConfig.setRewriteNamespaceDelimiter("--&");
        assertEquals("--&", (connectorConfig.getRewriteNamespaceDelimiter()));
    }

    @Test
    public void testDefaultBookkeeperConfig()
    {
        int availableProcessors = Runtime.getRuntime().availableProcessors();
        PulsarConnectorConfig connectorConfig = new PulsarConnectorConfig();
        assertEquals(0, connectorConfig.getBookkeeperThrottleValue());
        assertEquals(2 * availableProcessors, connectorConfig.getBookkeeperNumIOThreads());
        assertEquals(availableProcessors, connectorConfig.getBookkeeperNumWorkerThreads());
    }

    @Test
    public void testDefaultManagedLedgerConfig()
    {
        int availableProcessors = Runtime.getRuntime().availableProcessors();
        PulsarConnectorConfig connectorConfig = new PulsarConnectorConfig();
        assertEquals(0L, connectorConfig.getManagedLedgerCacheSizeMB());
        assertEquals(availableProcessors, connectorConfig.getManagedLedgerOffloadMaxThreads());
        assertEquals(availableProcessors, connectorConfig.getManagedLedgerNumSchedulerThreads());
        assertEquals(connectorConfig.getMaxSplitQueueSizeBytes(), -1);
    }

    @Test
    public void testGetOffloadPolices() throws Exception
    {
        PulsarConnectorConfig connectorConfig = new PulsarConnectorConfig();

        final String managedLedgerOffloadDriver = "s3";
        final String offloaderDirectory = "/pulsar/offloaders";
        final Integer managedLedgerOffloadMaxThreads = 5;
        final String bucket = "offload-bucket";
        final String region = "us-west-2";
        final String endpoint = "http://s3.amazonaws.com";
        final String offloadProperties = "{"
                + "\"s3ManagedLedgerOffloadBucket\":\"" + bucket + "\","
                + "\"s3ManagedLedgerOffloadRegion\":\"" + region + "\","
                + "\"s3ManagedLedgerOffloadServiceEndpoint\":\"" + endpoint + "\""
                + "}";

        connectorConfig.setManagedLedgerOffloadDriver(managedLedgerOffloadDriver);
        connectorConfig.setOffloadersDirectory(offloaderDirectory);
        connectorConfig.setManagedLedgerOffloadMaxThreads(managedLedgerOffloadMaxThreads);
        connectorConfig.setOffloaderProperties(offloadProperties);

        OffloadPolicies offloadPolicies = connectorConfig.getOffloadPolices();
        assertNotNull(offloadPolicies);
        assertEquals(offloadPolicies.getManagedLedgerOffloadDriver(), managedLedgerOffloadDriver);
        assertEquals(offloadPolicies.getOffloadersDirectory(), offloaderDirectory);
        assertEquals((int) offloadPolicies.getManagedLedgerOffloadMaxThreads(), (int) managedLedgerOffloadMaxThreads);
        assertEquals(offloadPolicies.getS3ManagedLedgerOffloadBucket(), bucket);
        assertEquals(offloadPolicies.getS3ManagedLedgerOffloadRegion(), region);
        assertEquals(offloadPolicies.getS3ManagedLedgerOffloadServiceEndpoint(), endpoint);
    }
}
