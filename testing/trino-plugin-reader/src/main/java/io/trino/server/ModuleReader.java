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

import com.google.common.io.ByteStreams;
import io.trino.spi.Plugin;
import org.apache.maven.model.Model;
import org.apache.maven.model.io.xpp3.MavenXpp3Reader;
import org.codehaus.plexus.util.xml.pull.XmlPullParserException;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;
import java.util.Map;
import java.util.MissingResourceException;
import java.util.function.BiPredicate;
import java.util.stream.Stream;
import java.util.zip.ZipFile;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNullElse;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

public class ModuleReader
{
    private ModuleReader() {}

    public static Map<String, String> mapModulesToPlugins(File rootPom)
    {
        List<String> modules = readTrinoPlugins(rootPom);
        return modules.stream()
                .collect(toMap(identity(), module -> readPluginClassName(rootPom, module)));
    }

    private static List<String> readTrinoPlugins(File rootPom)
    {
        try (FileReader fileReader = new FileReader(rootPom, UTF_8)) {
            MavenXpp3Reader reader = new MavenXpp3Reader();
            Model model = reader.read(fileReader);
            return model.getModules().stream()
                    .filter(module -> isTrinoPlugin(requireNonNullElse(rootPom.getParent(), ".") + "/" + module))
                    .collect(toImmutableList());
        }
        catch (IOException e) {
            throw new UncheckedIOException(format("Couldn't read file %s", rootPom), e);
        }
        catch (XmlPullParserException e) {
            throw new RuntimeException(format("Couldn't parse file %s", rootPom), e);
        }
    }

    private static boolean isTrinoPlugin(String module)
    {
        String modulePom = module + "/pom.xml";
        try (FileReader fileReader = new FileReader(modulePom, UTF_8)) {
            MavenXpp3Reader reader = new MavenXpp3Reader();
            Model model = reader.read(fileReader);
            return model.getPackaging().equals("trino-plugin");
        }
        catch (IOException e) {
            throw new UncheckedIOException(format("Couldn't read file %s", modulePom), e);
        }
        catch (XmlPullParserException e) {
            throw new RuntimeException(format("Couldn't parse file %s", modulePom), e);
        }
    }

    private static String readPluginClassName(File rootPom, String module)
    {
        Path target = Path.of(requireNonNullElse(rootPom.getParent(), "."), module, "target");
        BiPredicate<Path, BasicFileAttributes> matcher = (path, attributes) -> path.toFile().getName().matches(".*-services\\.jar");
        try (Stream<Path> files = Files.find(target, 1, matcher)) {
            return files.findFirst()
                    .map(jarFile -> readPluginClassName(jarFile.toFile()))
                    .orElseThrow(() -> new MissingResourceException(
                            format("Couldn't find plugin name in services jar for module %s", module),
                            Plugin.class.getName(),
                            module));
        }
        catch (IOException e) {
            throw new UncheckedIOException(format("Couldn't read services jar for module %s", module), e);
        }
    }

    private static String readPluginClassName(File serviceJar)
    {
        try {
            ZipFile zipFile = new ZipFile(serviceJar);
            return zipFile.stream()
                    .filter(entry -> !entry.isDirectory() && entry.getName().equals("META-INF/services/io.trino.spi.Plugin"))
                    .findFirst()
                    .map(entry -> {
                        try (BufferedInputStream bis = new BufferedInputStream(zipFile.getInputStream(entry))) {
                            return new String(ByteStreams.toByteArray(bis), UTF_8).trim();
                        }
                        catch (IOException e) {
                            throw new UncheckedIOException(format("Couldn't read plugin's service descriptor in %s", serviceJar), e);
                        }
                    })
                    .orElseThrow(() -> new MissingResourceException(
                            format("Couldn't find 'META-INF/services/io.trino.spi.Plugin' file in the service JAR %s", serviceJar.getPath()),
                            Plugin.class.getName(),
                            serviceJar.getPath()));
        }
        catch (IOException e) {
            throw new UncheckedIOException(format("Couldn't process service JAR %s", serviceJar), e);
        }
    }
}
