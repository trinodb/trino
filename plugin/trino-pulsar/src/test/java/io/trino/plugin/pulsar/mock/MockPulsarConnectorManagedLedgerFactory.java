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
package io.trino.plugin.pulsar.mock;

import io.trino.plugin.pulsar.PulsarConnectorCache;
import io.trino.plugin.pulsar.PulsarConnectorConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerFactory;
import org.apache.bookkeeper.stats.StatsProvider;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.policies.data.OffloadPoliciesImpl;

public class MockPulsarConnectorManagedLedgerFactory
        implements PulsarConnectorCache
{
    private ManagedLedgerFactory managedLedgerFactory;

    public MockPulsarConnectorManagedLedgerFactory(ManagedLedgerFactory managedLedgerFactory) throws Exception
    {
        this.managedLedgerFactory = managedLedgerFactory;
    }

    @Override
    public ManagedLedgerFactory getManagedLedgerFactory()
    {
        return managedLedgerFactory;
    }

    @Override
    public ManagedLedgerConfig getManagedLedgerConfig(NamespaceName namespaceName, OffloadPoliciesImpl offloadPolicies, PulsarConnectorConfig pulsarConnectorConfig)
    {
        return null;
    }

    @Override
    public StatsProvider getStatsProvider()
    {
        return null;
    }
}
