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
import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FileDescriptor;
import io.airlift.units.Duration;
import io.trino.hive.formats.line.Column;
import io.trino.hive.formats.line.LineDeserializerFactory;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.AbstractScheduledService.Scheduler.newFixedRateSchedule;
import static io.trino.hive.formats.line.protobuf.ProtobufConstants.HIVE_SERDE_CLASS_NAMES;
import static java.util.Objects.requireNonNull;

public class ProtobufDeserializerFactory
        implements LineDeserializerFactory
{
    private final FileDescriptorSetsLoaderScheduler loader;

    public ProtobufDeserializerFactory(Path descriptorsDirectory, Duration updateInterval)
    {
        loader = descriptorsDirectory != null ? new FileDescriptorSetsLoaderScheduler(descriptorsDirectory, updateInterval) : null;
    }

    @Override
    public Set<String> getHiveSerDeClassNames()
    {
        return HIVE_SERDE_CLASS_NAMES;
    }

    @Override
    public ProtobufDeserializer create(List<Column> columns, Map<String, String> serdeProperties)
    {
        requireNonNull(this.loader, "No \"hive.protobufs.descriptors\" set in hive configuration");

        String serializationClass = serdeProperties.get("serialization.class");
        requireNonNull(serializationClass, "serialization.class");
        checkArgument(serializationClass.contains("$"), "Expected serialization.class to contain {package}${protoname}, but was %s", serializationClass);
        List<String> javaClassNameAndProtoName = Splitter.on('$').limit(2).splitToList(serializationClass);

        Optional<Descriptor> descriptor = loader.getFileDescriptorSets().stream()
                .map(fileDescriptorSet -> findDescriptor(fileDescriptorSet, javaClassNameAndProtoName.getFirst(), javaClassNameAndProtoName.getLast()))
                .filter(Objects::nonNull)
                .findFirst();

        checkArgument(descriptor.isPresent(), "No file descriptor found for serialization class %s in directory %s", serializationClass, loader.getDescriptorsDirectory());
        return new ProtobufDeserializer(columns, descriptor.get());
    }

    Descriptor findDescriptor(FileDescriptorSet fileDescriptorSet, String javaClassName, String protoName)
    {
        List<FileDescriptor> loaded = new ArrayList<>();
        for (FileDescriptorProto proto : fileDescriptorSet.getFileList()) {
            try {
                FileDescriptor fileDescriptor = FileDescriptor.buildFrom(proto, loaded.toArray(FileDescriptor[]::new));
                loaded.add(fileDescriptor);

                if (javaClassName.equals(fileDescriptor.getOptions().getJavaPackage() + "." + fileDescriptor.getOptions().getJavaOuterClassname())) {
                    Descriptor descriptor = fileDescriptor.findMessageTypeByName(protoName);
                    if (descriptor != null) {
                        return descriptor;
                    }
                }
            }
            catch (DescriptorValidationException e) {
                throw new IllegalStateException("Failed to load protobuf fileDescriptor " + proto.getName() + " from " + loader.getDescriptorsDirectory(), e);
            }
        }
        return null;
    }

    static class FileDescriptorSetsLoaderScheduler
            extends AbstractScheduledService
    {
        private final Duration interval;
        private final Path descriptorsDirectory;
        private final Map<Path, FileDescriptorSet> fileDescriptorSets = new HashMap<>();

        FileDescriptorSetsLoaderScheduler(Path descriptorsDirectory, Duration interval)
        {
            this.descriptorsDirectory = descriptorsDirectory;
            this.interval = interval;
            runOneIteration();
            startAsync();
        }

        @Override
        protected void runOneIteration()
        {
            // Load all protobuf descriptors from the directory.
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(descriptorsDirectory)) {
                for (Path path : stream) {
                    if (!Files.isDirectory(path)) {
                        try (InputStream in = Files.newInputStream(path)) {
                            fileDescriptorSets.put(path, FileDescriptorSet.parseFrom(in));
                        }
                    }
                }
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        public Collection<FileDescriptorSet> getFileDescriptorSets()
        {
            return fileDescriptorSets.values();
        }

        public Path getDescriptorsDirectory()
        {
            return descriptorsDirectory;
        }

        @Override
        protected @NotNull Scheduler scheduler()
        {
            return newFixedRateSchedule(interval.toMillis(), interval.toMillis(), TimeUnit.MILLISECONDS);
        }
    }
}
