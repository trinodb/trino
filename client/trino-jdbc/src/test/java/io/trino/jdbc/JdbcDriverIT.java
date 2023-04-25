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
package io.trino.jdbc;

import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Set;
import java.util.jar.JarFile;
import java.util.zip.ZipEntry;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.assertj.core.api.Assertions.assertThat;

public class JdbcDriverIT
{
    private static final Set<String> MANIFEST_FILES = ImmutableSet.of(
            "META-INF/MANIFEST.MF",
            "META-INF/services/java.sql.Driver");

    @Parameters("jdbc-jar")
    @Test
    public void testDependenciesRelocated(String file)
    {
        try (JarFile jarFile = new JarFile(file)) {
            List<String> nonRelocatedFiles = jarFile.stream()
                    .filter(value -> !value.isDirectory())
                    .map(ZipEntry::getName)
                    .filter(name -> !isExpectedFile(name))
                    .collect(toImmutableList());

            assertThat(nonRelocatedFiles)
                    .describedAs("Non-relocated files in the shaded jar")
                    .isEmpty();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static boolean isExpectedFile(String filename)
    {
        return MANIFEST_FILES.contains(filename) || filename.startsWith("io/trino/jdbc");
    }
}
