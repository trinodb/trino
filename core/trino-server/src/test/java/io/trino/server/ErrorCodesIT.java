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
package io.trino.server;

import com.google.common.collect.ImmutableSet;
import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.ErrorType;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.lang.classfile.ClassFile;
import java.lang.classfile.ClassModel;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.jar.JarFile;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

import static io.trino.server.PluginManager.createClassLoader;
import static java.lang.reflect.AccessFlag.ENUM;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public class ErrorCodesIT
{
    // These libraries deliberately expose the same errors as the Hive connector.
    private static final Set<Set<String>> SHARED_ERROR_CODES = ImmutableSet.of(
            ImmutableSet.of(
                    "io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_METADATA",
                    "io.trino.hive.formats.HiveFormatsErrorCode.HIVE_INVALID_METADATA",
                    "io.trino.metastore.MetastoreErrorCode.HIVE_INVALID_METADATA"),
            ImmutableSet.of(
                    "io.trino.plugin.hive.HiveErrorCode.HIVE_UNSUPPORTED_FORMAT",
                    "io.trino.metastore.MetastoreErrorCode.HIVE_UNSUPPORTED_FORMAT"),
            ImmutableSet.of(
                    "io.trino.plugin.hive.HiveErrorCode.HIVE_UNSERIALIZABLE_JSON_VALUE",
                    "io.trino.hive.formats.HiveFormatsErrorCode.HIVE_UNSERIALIZABLE_JSON_VALUE"));

    @Test
    public void testUniqueErrorCodes(@TempDir Path temporaryDirectory)
            throws Exception
    {
        Path archive = Path.of(requireNonNull(System.getProperty("trino-server-archive"), "trino-server-archive is not set"));
        assertThat(archive).isRegularFile();
        Set<Path> classPaths = unpackJars(archive, temporaryDirectory);
        assertThat(classPaths).anyMatch(path -> path.getFileName().toString().equals("lib"));
        assertThat(classPaths).anyMatch(path -> path.getParent().getFileName().toString().equals("plugin"));

        Map<String, ErrorCodeValue> errorCodes = new TreeMap<>();
        for (Path classPath : classPaths) {
            List<URL> urls = new ArrayList<>();
            try (Stream<Path> files = Files.list(classPath)) {
                for (Path jar : files.filter(Files::isRegularFile).filter(path -> path.toString().endsWith(".jar")).sorted().toList()) {
                    urls.add(jar.toUri().toURL());
                }
            }
            // Keep each plugin's dependencies isolated while sharing the SPI with the test.
            try (PluginClassLoader classLoader = createClassLoader(classPath.toString(), urls)) {
                for (URL url : urls) {
                    try (JarFile jar = new JarFile(Path.of(url.toURI()).toFile())) {
                        for (var entry : jar.stream().filter(entry -> entry.getName().startsWith("io/trino/") && entry.getName().endsWith(".class")).toList()) {
                            ClassModel model;
                            try (var input = jar.getInputStream(entry)) {
                                model = ClassFile.of().parse(input.readAllBytes());
                            }
                            if (!model.flags().has(ENUM)) {
                                continue;
                            }
                            Class<?> type = Class.forName(model.thisClass().asInternalName().replace('/', '.'), false, classLoader);
                            if (!type.isEnum() || !ErrorCodeSupplier.class.isAssignableFrom(type)) {
                                continue;
                            }
                            for (Object constant : type.getEnumConstants()) {
                                String definition = type.getName() + "." + ((Enum<?>) constant).name();
                                ErrorCodeValue value = new ErrorCodeValue(((ErrorCodeSupplier) constant).toErrorCode());
                                ErrorCodeValue previous = errorCodes.putIfAbsent(definition, value);
                                // Shared libraries occur in several plugin class loaders. Their definitions must agree.
                                if (previous != null) {
                                    assertThat(value).as("Error code %s in %s", definition, classPath).isEqualTo(previous);
                                }
                            }
                        }
                    }
                }
            }
        }
        assertThat(errorCodes).containsKey("io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR");

        Map<Integer, Set<String>> codes = new TreeMap<>();
        errorCodes.forEach((definition, value) -> codes.computeIfAbsent(value.code(), _ -> new TreeSet<>()).add(definition));
        codes.forEach((code, definitions) -> {
            if (definitions.size() > 1) {
                assertThat(SHARED_ERROR_CODES)
                        .as("Duplicate error code 0x%08x: %s", code, definitions)
                        .contains(definitions);
                assertThat(definitions.stream().map(errorCodes::get).distinct())
                        .as("Shared error code values for %s", definitions)
                        .hasSize(1);
            }
        });
    }

    private static Set<Path> unpackJars(Path archive, Path directory)
            throws IOException
    {
        Set<Path> classPaths = new TreeSet<>();
        try (var input = new TarArchiveInputStream(new GZIPInputStream(Files.newInputStream(archive)))) {
            TarArchiveEntry entry;
            while ((entry = input.getNextEntry()) != null) {
                if (!entry.getName().endsWith(".jar")) {
                    continue;
                }
                Path file = directory.resolve(entry.getName()).normalize();
                assertThat(file.startsWith(directory)).as("Archive entry %s", entry.getName()).isTrue();
                Files.createDirectories(file.getParent());
                if (entry.isLink()) {
                    Path target = directory.resolve(entry.getLinkName()).normalize();
                    assertThat(target.startsWith(directory)).as("Archive link %s", entry.getLinkName()).isTrue();
                    Files.createLink(file, target);
                }
                else {
                    assertThat(entry.isFile()).as("Archive entry %s must be a regular file", entry.getName()).isTrue();
                    Files.copy(input, file);
                }
                classPaths.add(file.getParent());
            }
        }
        return classPaths;
    }

    // ErrorCode.equals only compares the number, but repeated and shared definitions must also agree on metadata.
    private record ErrorCodeValue(int code, String name, ErrorType type, boolean fatal)
    {
        private ErrorCodeValue(ErrorCode errorCode)
        {
            this(errorCode.getCode(), errorCode.getName(), errorCode.getType(), errorCode.isFatal());
        }
    }
}
