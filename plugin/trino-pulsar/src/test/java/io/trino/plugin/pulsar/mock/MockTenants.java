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

import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.admin.Tenants;
import org.apache.pulsar.common.policies.data.TenantInfo;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public class MockTenants
        implements Tenants
{
    private List<String> tenants;

    public MockTenants(List<String> tenants)
    {
        this.tenants = tenants;
    }

    @Override
    public List<String> getTenants()
    {
        return tenants;
    }

    @Override
    public CompletableFuture<List<String>> getTenantsAsync()
    { return null; }

    @Override
    public TenantInfo getTenantInfo(String s) throws PulsarAdminException
    { return null; }

    @Override
    public CompletableFuture<TenantInfo> getTenantInfoAsync(String s)
    { return null; }

    @Override
    public void createTenant(String s, TenantInfo tenantInfo) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> createTenantAsync(String s, TenantInfo tenantInfo)
    { return null; }

    @Override
    public void updateTenant(String s, TenantInfo tenantInfo) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> updateTenantAsync(String s, TenantInfo tenantInfo)
    { return null; }

    @Override
    public void deleteTenant(String s) throws PulsarAdminException
    { }

    @Override
    public void deleteTenant(String tenant, boolean force) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> deleteTenantAsync(String s)
    { return null; }

    @Override
    public CompletableFuture<Void> deleteTenantAsync(String tenant, boolean force)
    {
        return null;
    }
}
