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

import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.airlift.log.Logger;
import io.trino.cache.EvictableCacheBuilder;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.SchemaInfoProvider;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.protocol.schema.BytesSchemaVersion;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.util.FutureUtil;
import org.glassfish.jersey.internal.inject.InjectionManagerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.CompletableFuture.completedFuture;

/**
 * Multi version schema info provider for Pulsar SQL leverage guava cache.
 */
public class PulsarSchemaInfoProvider
            implements SchemaInfoProvider
{
    private static final Logger LOG = Logger.get(PulsarSchemaInfoProvider.class);

    private final TopicName topicName;

    private final PulsarConnectorConfig pulsarConnectorConfig;

    private final LoadingCache<BytesSchemaVersion, SchemaInfo> cache = EvictableCacheBuilder.newBuilder().maximumSize(100000)
            .expireAfterWrite(30, TimeUnit.MINUTES).build(new CacheLoader<BytesSchemaVersion, SchemaInfo>() {
                @SuppressWarnings("null")
                @Override
                public SchemaInfo load(BytesSchemaVersion schemaVersion)
                         throws Exception
                {
                    return loadSchema(schemaVersion);
                }
            });

    public PulsarSchemaInfoProvider(TopicName topicName, PulsarConnectorConfig pulsarConnectorConfig)
    {
        this.topicName = topicName;
        this.pulsarConnectorConfig = pulsarConnectorConfig;
    }

    public static SchemaInfo defaultSchema()
    {
        return Schema.BYTES.getSchemaInfo();
    }

    @Override
    public CompletableFuture<SchemaInfo> getSchemaByVersion(byte[] schemaVersion)
    {
        try {
            if (null == schemaVersion) {
                try (PulsarAdmin pulsarAdmin = PulsarAdminClientProvider.getPulsarAdmin(pulsarConnectorConfig)) {
                    return completedFuture(pulsarAdmin.schemas().getSchemaInfoWithVersion(topicName.toString()).getSchemaInfo());
                }
                catch (PulsarAdminException | PulsarClientException e) {
                    return FutureUtil.failedFuture(e.getCause());
                }
            }
            return completedFuture(cache.get(BytesSchemaVersion.of(schemaVersion)));
        }
        catch (ExecutionException e) {
            LOG.error("Can't get generic schema for topic {} schema version {}",
                    topicName.toString(), new String(schemaVersion, StandardCharsets.UTF_8), e);
            return FutureUtil.failedFuture(e.getCause());
        }
    }

    @Override
    public CompletableFuture<SchemaInfo> getLatestSchema()
    {
        try (PulsarAdmin pulsarAdmin = PulsarAdminClientProvider.getPulsarAdmin(pulsarConnectorConfig)) {
            return completedFuture(pulsarAdmin.schemas()
                    .getSchemaInfo(topicName.toString()));
        }
        catch (PulsarAdminException | PulsarClientException e) {
            return FutureUtil.failedFuture(e);
        }
    }

    @Override
    public String getTopicName()
    {
        return topicName.getLocalName();
    }

    private SchemaInfo loadSchema(BytesSchemaVersion bytesSchemaVersion)
            throws PulsarAdminException, PulsarClientException
    {
        ClassLoader originalContextLoader = Thread.currentThread().getContextClassLoader();
        try (PulsarAdmin pulsarAdmin = PulsarAdminClientProvider.getPulsarAdmin(pulsarConnectorConfig)) {
            Thread.currentThread().setContextClassLoader(InjectionManagerFactory.class.getClassLoader());
            return pulsarAdmin.schemas()
                    .getSchemaInfo(topicName.toString(), ByteBuffer.wrap(bytesSchemaVersion.get()).getLong());
        }
        finally {
            Thread.currentThread().setContextClassLoader(originalContextLoader);
        }
    }
}
