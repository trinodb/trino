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
import org.apache.pulsar.client.admin.Schemas;
import org.apache.pulsar.common.protocol.schema.IsCompatibilityResponse;
import org.apache.pulsar.common.protocol.schema.PostSchemaPayload;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaInfoWithVersion;

import javax.ws.rs.ClientErrorException;
import javax.ws.rs.core.Response;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class MockSchemas
        implements Schemas
{
    private Map<String, SchemaInfo> topicsToSchemas;

    public MockSchemas(Map<String, SchemaInfo> topicsToSchemas)
    {
        this.topicsToSchemas = topicsToSchemas;
    }

    @Override
    public SchemaInfo getSchemaInfo(String topic) throws PulsarAdminException
    {
        if (topicsToSchemas.get(topic) != null) {
            return topicsToSchemas.get(topic);
        }
        else {
            ClientErrorException cee = new ClientErrorException(Response.Status.NOT_FOUND);
            throw new PulsarAdminException(cee, cee.getMessage(), cee.getResponse().getStatus());
        }
    }

    @Override
    public CompletableFuture<SchemaInfo> getSchemaInfoAsync(String s)
    {
        return null;
    }

    @Override
    public SchemaInfoWithVersion getSchemaInfoWithVersion(String s) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<SchemaInfoWithVersion> getSchemaInfoWithVersionAsync(String s)
    {
        return null;
    }

    @Override
    public SchemaInfo getSchemaInfo(String s, long l) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<SchemaInfo> getSchemaInfoAsync(String s, long l)
    {
        return null;
    }

    @Override
    public void deleteSchema(String s) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> deleteSchemaAsync(String s)
    {
        return null;
    }

    @Override
    public void createSchema(String s, SchemaInfo schemaInfo) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> createSchemaAsync(String s, SchemaInfo schemaInfo)
    {
        return null;
    }

    @Override
    public void createSchema(String s, PostSchemaPayload postSchemaPayload) throws PulsarAdminException
    { }

    @Override
    public CompletableFuture<Void> createSchemaAsync(String s, PostSchemaPayload postSchemaPayload)
    {
        return null;
    }

    @Override
    public IsCompatibilityResponse testCompatibility(String s, PostSchemaPayload postSchemaPayload) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<IsCompatibilityResponse> testCompatibilityAsync(String s, PostSchemaPayload postSchemaPayload)
    {
        return null;
    }

    @Override
    public Long getVersionBySchema(String s, PostSchemaPayload postSchemaPayload) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<Long> getVersionBySchemaAsync(String s, PostSchemaPayload postSchemaPayload)
    {
        return null;
    }

    @Override
    public IsCompatibilityResponse testCompatibility(String s, SchemaInfo schemaInfo) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<IsCompatibilityResponse> testCompatibilityAsync(String s, SchemaInfo schemaInfo)
    {
        return null;
    }

    @Override
    public Long getVersionBySchema(String s, SchemaInfo schemaInfo) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<Long> getVersionBySchemaAsync(String s, SchemaInfo schemaInfo)
    {
        return null;
    }

    @Override
    public List<SchemaInfo> getAllSchemas(String s) throws PulsarAdminException
    {
        return null;
    }

    @Override
    public CompletableFuture<List<SchemaInfo>> getAllSchemasAsync(String s)
    {
        return null;
    }
}
