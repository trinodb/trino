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
package io.trino.plugin.kafka.schema.confluent;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.protobuf.Descriptors.Descriptor;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider;
import io.trino.decoder.protobuf.DescriptorProvider;
import io.trino.spi.TrinoException;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static io.trino.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static java.util.Objects.requireNonNull;

public class ConfluentDescriptorProvider
        implements DescriptorProvider
{
    private final LoadingCache<String, Descriptor> protobufTypeUrlCache;

    public ConfluentDescriptorProvider()
    {
        protobufTypeUrlCache = buildNonEvictableCache(
                CacheBuilder.newBuilder().maximumSize(1000),
                CacheLoader.from(this::loadDescriptorFromType));
    }

    @Override
    public Optional<Descriptor> getDescriptorFromTypeUrl(String url)
    {
        try {
            requireNonNull(url, "url is null");
            return Optional.of(protobufTypeUrlCache.get(url));
        }
        catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private Descriptor loadDescriptorFromType(String url)
    {
        try {
            return ((ProtobufSchema) new ProtobufSchemaProvider()
                    .parseSchema(getContents(url), List.of(), true)
                    .orElseThrow())
                    .toDescriptor();
        }
        catch (NoSuchElementException e) {
            throw new TrinoException(GENERIC_USER_ERROR, "Failed to parse protobuf schema");
        }
    }
}
