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

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Bytes;
import io.trino.plugin.kafka.KafkaPlugin;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The Confluent protobuf provider, serializer and types libraries are available under the
 * Confluent Community License only. The connector must not reference them, so that the
 * protobuf support works without copying those libraries into the plugin directory.
 */
public class TestNoConfluentProtobufDependency
{
    private static final List<String> FORBIDDEN_REFERENCES = ImmutableList.of(
            "io/confluent/kafka/schemaregistry/protobuf/",
            "io.confluent.kafka.schemaregistry.protobuf.",
            "io/confluent/kafka/serializers/protobuf/",
            "io.confluent.kafka.serializers.protobuf.",
            "io/confluent/protobuf/",
            "io.confluent.protobuf.");

    @Test
    public void testConnectorClassesDoNotReferenceConfluentProtobufLibraries()
            throws Exception
    {
        List<String> classes = productionClasses();
        assertThat(classes).isNotEmpty();
        assertThat(classes).anyMatch(name -> name.endsWith("ConfluentProtobufSchemaProvider.class"));

        for (String className : classes) {
            byte[] bytecode = readProductionClass(className);
            for (String reference : FORBIDDEN_REFERENCES) {
                assertThat(Bytes.indexOf(bytecode, reference.getBytes(US_ASCII)))
                        .as("%s references %s", className, reference)
                        .isEqualTo(-1);
            }
        }
    }

    private static Path codeSource()
            throws URISyntaxException
    {
        return Path.of(KafkaPlugin.class.getProtectionDomain().getCodeSource().getLocation().toURI());
    }

    private static List<String> productionClasses()
            throws IOException, URISyntaxException
    {
        Path location = codeSource();
        if (Files.isDirectory(location)) {
            try (Stream<Path> files = Files.walk(location)) {
                return files
                        .filter(file -> file.toString().endsWith(".class"))
                        .map(file -> location.relativize(file).toString())
                        .collect(toImmutableList());
            }
        }
        try (JarFile jar = new JarFile(location.toFile())) {
            return jar.stream()
                    .map(JarEntry::getName)
                    .filter(name -> name.endsWith(".class"))
                    .collect(toImmutableList());
        }
    }

    private static byte[] readProductionClass(String className)
            throws IOException, URISyntaxException
    {
        Path location = codeSource();
        if (Files.isDirectory(location)) {
            return Files.readAllBytes(location.resolve(className));
        }
        try (JarFile jar = new JarFile(location.toFile());
                InputStream input = jar.getInputStream(jar.getJarEntry(className))) {
            return input.readAllBytes();
        }
    }
}
