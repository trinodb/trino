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
package io.trino.decoder.protobuf;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;
import io.trino.spi.TrinoException;

import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.decoder.protobuf.ProtobufErrorCode.INVALID_PROTO_FILE;
import static io.trino.decoder.protobuf.ProtobufRowDecoderFactory.DEFAULT_MESSAGE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class FileDescriptorProvider
        implements DescriptorProvider
{
    private final LoadingCache<String, Descriptor> protobufTypeUrlCache;

    public FileDescriptorProvider()
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
            Descriptor descriptor = ProtobufUtils.getFileDescriptor(getContents(url)).findMessageTypeByName(DEFAULT_MESSAGE);
            checkState(descriptor != null, format("Message %s not found", DEFAULT_MESSAGE));
            return descriptor;
        }
        catch (Descriptors.DescriptorValidationException e) {
            throw new TrinoException(INVALID_PROTO_FILE, "Unable to parse protobuf schema", e);
        }
    }
}
