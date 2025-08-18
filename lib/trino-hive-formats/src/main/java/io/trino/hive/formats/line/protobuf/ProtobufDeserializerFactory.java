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
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FileDescriptor;
import io.airlift.units.Duration;
import io.trino.hive.formats.line.Column;
import io.trino.hive.formats.line.LineDeserializerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.cache.CacheBuilder.newBuilder;
import static com.google.common.util.concurrent.MoreExecutors.listeningDecorator;
import static com.google.protobuf.DescriptorProtos.FileDescriptorSet.parseFrom;
import static com.google.protobuf.Descriptors.FileDescriptor.buildFrom;
import static io.trino.hive.formats.line.protobuf.ProtobufConstants.HIVE_SERDE_CLASS_NAMES;
import static java.nio.file.Files.isDirectory;
import static java.nio.file.Files.newDirectoryStream;
import static java.nio.file.Files.newInputStream;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;

public class ProtobufDeserializerFactory
        implements LineDeserializerFactory
{
    private LoadingCache<String, Descriptor> cache;

    public ProtobufDeserializerFactory(Path descriptorsDirectory, Duration updateInterval)
    {
        if (descriptorsDirectory != null) {
            // When no directory is set in the config, the factory will fail when create() is used
            // as the cache is null
            cache = newBuilder()
                    .refreshAfterWrite(updateInterval.toJavaTime())
                    .build(new DescriptorCacheLoader(descriptorsDirectory));
        }
    }

    @Override
    public Set<String> getHiveSerDeClassNames()
    {
        return HIVE_SERDE_CLASS_NAMES;
    }

    @Override
    public ProtobufDeserializer create(List<Column> columns, Map<String, String> serdeProperties)
    {
        requireNonNull(this.cache, "No \"hive.protobufs.descriptors\" set in hive configuration");

        String serializationClass = serdeProperties.get("serialization.class");
        return new ProtobufDeserializer(columns, cache.getUnchecked(serializationClass));
    }

    private static class DescriptorCacheLoader
            extends CacheLoader<String, Descriptor>
    {
        private final Path descriptorsDirectory;
        private final ListeningExecutorService executor = listeningDecorator(newFixedThreadPool(1));

        DescriptorCacheLoader(Path descriptorsDirectory)
        {
            this.descriptorsDirectory = descriptorsDirectory;
        }

        @Override
        public Descriptor load(String serializationClass)
                throws IOException
        {
            requireNonNull(serializationClass, "serialization.class");
            checkArgument(serializationClass.matches("^[^$]+\\$[^$]+$"), "Expected serialization.class to contain {package}${protoname}, but was %s", serializationClass);
            List<String> javaClassAndProtoName = Splitter.on('$').limit(2).splitToList(serializationClass);

            try (DirectoryStream<Path> stream = newDirectoryStream(descriptorsDirectory)) {
                for (Path path : stream) {
                    if (!isDirectory(path)) {
                        try (InputStream in = newInputStream(path)) {
                            FileDescriptorSet fileDescriptorSet = parseFrom(in);

                            List<FileDescriptor> dependencies = new ArrayList<>();
                            for (FileDescriptorProto proto : fileDescriptorSet.getFileList()) {
                                try {
                                    FileDescriptor fileDescriptor = buildFrom(proto, dependencies.toArray(FileDescriptor[]::new));
                                    dependencies.add(fileDescriptor);

                                    if (javaClassAndProtoName.getFirst().equals(fileDescriptor.getOptions().getJavaPackage() + "." + fileDescriptor.getOptions().getJavaOuterClassname())) {
                                        Descriptor descriptor = fileDescriptor.findMessageTypeByName(javaClassAndProtoName.getLast());
                                        if (descriptor != null) {
                                            return descriptor;
                                        }
                                    }
                                }
                                catch (DescriptorValidationException e) {
                                    throw new IllegalStateException("Failed to load protobuf fileDescriptor " + proto.getName() + " from " + path, e);
                                }
                            }
                        }
                    }
                }
            }
            throw new IllegalArgumentException("No file descriptor found for serialization class " + serializationClass);
        }

        public ListenableFuture<Descriptor> reload(String serializationClass, Descriptor oldValue)
        {
            checkNotNull(serializationClass);
            checkNotNull(oldValue);

            SettableFuture<Descriptor> future = SettableFuture.create();
            // This executor executes using only one thread at a time,
            // to avoid a mass file read when all descriptors are  refreshed at the same time or the file I/O is slow.
            executor.execute(() -> {
                try {
                    future.set(DescriptorCacheLoader.this.load(serializationClass));
                }
                catch (IOException e) {
                    future.setException(e);
                }
            });
            return future;
        }
    }
}
