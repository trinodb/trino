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
package io.trino.hive.formats.line.protobuf;

import com.google.common.base.Splitter;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FileDescriptor;
import io.airlift.units.Duration;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.hive.formats.line.Column;
import io.trino.hive.formats.line.LineDeserializerFactory;
import io.trino.spi.TrinoException;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.trino.hive.formats.HiveClassNames.TWITTER_ELEPHANTBIRD_PROTOBUF_SERDE_CLASS;
import static io.trino.hive.formats.HiveFormatsErrorCode.HIVE_INVALID_METADATA;
import static io.trino.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static java.nio.file.Files.isDirectory;
import static java.nio.file.Files.newDirectoryStream;
import static java.nio.file.Files.newInputStream;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

public class ProtobufDeserializerFactory
        implements LineDeserializerFactory
{
    private LoadingCache<String, Descriptor> cache;

    public ProtobufDeserializerFactory(Path descriptorsDirectory, Duration updateInterval, long maximumSize)
    {
        if (descriptorsDirectory != null) {
            // When no directory is set in the config, the factory will fail when create() is used
            // as the cache is null
            cache = EvictableCacheBuilder.newBuilder()
                    .refreshAfterWrite(updateInterval.toJavaTime())
                    .maximumSize(maximumSize)
                    .build(new DescriptorCacheLoader(descriptorsDirectory));
        }
    }

    @Override
    public Set<String> getHiveSerDeClassNames()
    {
        return ImmutableSet.of(TWITTER_ELEPHANTBIRD_PROTOBUF_SERDE_CLASS);
    }

    @Override
    public ProtobufDeserializer create(List<Column> columns, Map<String, String> serdeProperties)
    {
        if (this.cache == null) {
            throw new TrinoException(CONFIGURATION_INVALID, "No \"hive.protobufs.descriptors\" set in hive configuration");
        }

        String serializationClass = serdeProperties.get("serialization.class");
        if (serializationClass == null) {
            throw new TrinoException(HIVE_INVALID_METADATA, "Missing serdeproperties key \"serialization.class\"");
        }
        else if (!serializationClass.matches("^[^$]+\\$[^$]+$")) {
            throw new TrinoException(HIVE_INVALID_METADATA, String.format("Expected serialization.class to contain {package}${protoname}, but was %s", serializationClass));
        }

        return new ProtobufDeserializer(columns, cache.getUnchecked(serializationClass));
    }

    private static class DescriptorCacheLoader
            extends CacheLoader<String, Descriptor>
    {
        private final Path descriptorsDirectory;
        private final ListeningExecutorService executor = listeningDecorator(newSingleThreadExecutor(daemonThreadsNamed("protobuf-deserializer-%d")));

        DescriptorCacheLoader(Path descriptorsDirectory)
        {
            this.descriptorsDirectory = requireNonNull(descriptorsDirectory, "descriptorsDirectory is null");
        }

        @Override
        public Descriptor load(String serializationClass)
                throws IOException
        {
            List<String> javaClassAndProtoName = Splitter.on('$').limit(2).splitToList(serializationClass);

            try (DirectoryStream<Path> stream = newDirectoryStream(descriptorsDirectory)) {
                for (Path path : stream) {
                    if (!isDirectory(path)) {
                        try (InputStream in = newInputStream(path)) {
                            FileDescriptorSet fileDescriptorSet = FileDescriptorSet.parseFrom(in);

                            List<FileDescriptor> dependencies = new ArrayList<>();
                            for (FileDescriptorProto proto : fileDescriptorSet.getFileList()) {
                                try {
                                    FileDescriptor fileDescriptor = FileDescriptor.buildFrom(proto, dependencies.toArray(FileDescriptor[]::new));
                                    dependencies.add(fileDescriptor);

                                    if (javaClassAndProtoName.getFirst().equals(fileDescriptor.getOptions().getJavaPackage() + "." + fileDescriptor.getOptions().getJavaOuterClassname())) {
                                        Descriptor descriptor = fileDescriptor.findMessageTypeByName(javaClassAndProtoName.getLast());
                                        if (descriptor != null) {
                                            return descriptor;
                                        }
                                    }
                                }
                                catch (DescriptorValidationException e) {
                                    throw new TrinoException(CONFIGURATION_INVALID, String.format("Failed to load protobuf fileDescriptor %s from %s", proto.getName(), path), e);
                                }
                            }
                        }
                    }
                }
            }
            throw new TrinoException(HIVE_INVALID_METADATA, String.format("No file descriptor found for serialization class %s", serializationClass));
        }

        @Override
        public ListenableFuture<Descriptor> reload(String serializationClass, Descriptor oldValue)
        {
            requireNonNull(serializationClass);
            requireNonNull(oldValue);

            // This executor executes using only one thread at a time,
            // to avoid a mass file read when all descriptors are  refreshed at the same time or the file I/O is slow.
            return executor.submit(() -> DescriptorCacheLoader.this.load(serializationClass));
        }
    }
}
