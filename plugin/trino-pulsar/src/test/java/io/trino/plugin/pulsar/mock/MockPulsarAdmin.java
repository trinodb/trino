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

import org.apache.pulsar.client.admin.Namespaces;
import org.apache.pulsar.client.admin.Schemas;
import org.apache.pulsar.client.admin.Tenants;
import org.apache.pulsar.client.admin.Topics;
import org.apache.pulsar.client.admin.internal.PulsarAdminImpl;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;

public class MockPulsarAdmin
        extends PulsarAdminImpl
{
    private Tenants tenants;

    private Namespaces namespaces;

    private Topics topics;

    private Schemas schemas;

    public MockPulsarAdmin(String serviceUrl, ClientConfigurationData clientConfigData,
                           Tenants tenants, Namespaces namespaces, Topics topics,
                           Schemas schemas) throws PulsarClientException
    {
        super(serviceUrl, clientConfigData);
        this.tenants = tenants;
        this.namespaces = namespaces;
        this.topics = topics;
        this.schemas = schemas;
    }

    @Override
    public Tenants tenants()
    {
        return tenants;
    }

    @Override
    public Namespaces namespaces()
    {
        return namespaces;
    }

    @Override
    public Topics topics()
    {
        return topics;
    }

    @Override
    public Schemas schemas()
    {
        return schemas;
    }
}
