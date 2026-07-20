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
package io.trino.server.assembler;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Builds a Trino server distribution archive from staged source artifacts.
 * <p>
 * Two inputs describe the distribution. The staging directory mirrors the layout
 * for the handful of artifacts whose destination is not implied by their name: the
 * launcher, the secrets plugin, the server jars in {@code lib} and loose files such
 * as {@code NOTICE}. The plugins directory holds connector archives named
 * {@code trino-<name>.zip}, each of which becomes {@code plugin/<name>}; the pom's
 * dependency list is therefore the only place plugins are enumerated.
 * <p>
 * Archives are streamed straight into the output rather than expanded to disk
 * first. Expanding would write several gigabytes, most of it duplicate jars that
 * end up as hard link entries anyway. Pass {@code --explode} to also materialise
 * the tree, which the plugin reader needs in CI.
 */
@Command(name = "server-assembler", description = "Assembles a Trino server distribution archive")
public final class ServerAssembler
{
    private static final String PLUGIN_PREFIX = "trino-";

    @Option(names = "--stage", required = true, description = "Artifacts staged in the layout of the distribution")
    private Path stageDirectory;

    @Option(names = "--plugins", description = "Plugin archives named trino-<name>.zip")
    private Path pluginsDirectory;

    @Option(names = "--output", required = true, description = "Archive to write")
    private Path outputFile;

    @Option(names = "--root-directory", required = true, description = "Single directory inside the archive")
    private String rootDirectory;

    @Option(names = "--timestamp", required = true, description = "Modification time recorded for every entry")
    private Instant timestamp;

    @Option(names = "--explode", description = "Also write the distribution as a directory tree")
    private Path explodeDirectory;

    @Option(names = "--overlay", description = "Archive layered into other plugins, as artifact=plugin,plugin")
    private Map<String, String> overlays = Map.of();

    @Option(names = "--filter", description = "Staging relative path to substitute properties in")
    private List<String> filteredPaths = ImmutableList.of();

    @Option(names = "--property", description = "Property substituted into filtered files, as key=value")
    private Map<String, String> properties = Map.of();

    @Option(names = "--hardlink-include", description = "Glob of file names deduplicated into hard links")
    private List<String> hardLinkIncludes = ImmutableList.of("*.jar");

    @Option(names = "--compression-level", description = "Deflate level, 0 to 9")
    private int compressionLevel = DistributionWriter.DEFAULT_COMPRESSION_LEVEL;

    public static void main(String[] args)
            throws IOException
    {
        ServerAssembler assembler = new ServerAssembler();
        // parseArgs rather than execute, so failures propagate to Maven with their
        // original stack trace instead of becoming an exit code.
        new CommandLine(assembler).parseArgs(args);
        assembler.assemble();
    }

    private void assemble()
            throws IOException
    {
        checkArgument(Files.isDirectory(stageDirectory), "Staging directory does not exist: %s", stageDirectory);
        Files.createDirectories(outputFile.toAbsolutePath().getParent());

        long started = System.nanoTime();
        long entryCount;
        try (DistributionWriter writer = new DistributionWriter(
                outputFile,
                rootDirectory,
                timestamp,
                compressionLevel,
                compileGlobs(hardLinkIncludes),
                Optional.ofNullable(explodeDirectory),
                filteredPaths,
                properties)) {
            for (Path source : sources(stageDirectory)) {
                String name = relative(stageDirectory, source);
                if (isArchive(source)) {
                    writer.addArchive(parent(name), source, true);
                    continue;
                }
                writer.addFile(name, source);
            }

            for (Path archive : pluginArchives()) {
                String artifact = artifactName(archive);
                String overlaid = overlays.get(artifact);
                if (overlaid == null) {
                    writer.addArchive("plugin/" + artifact.substring(PLUGIN_PREFIX.length()), archive, true);
                    continue;
                }
                // An overlay is layered into other plugins and keeps its own root
                // directory, reproducing what Provisio's useRoot option produced.
                for (String plugin : Splitter.on(',').trimResults().omitEmptyStrings().split(overlaid)) {
                    writer.addArchive("plugin/" + plugin, archive, false);
                }
            }
            entryCount = writer.entryCount();
        }

        System.out.printf(
                "Assembled %s (%,d entries, %,d bytes) in %,d ms%n",
                outputFile.getFileName(),
                entryCount,
                Files.size(outputFile),
                NANOSECONDS.toMillis(System.nanoTime() - started));
    }

    /**
     * Lists staged sources in a stable order. Shallower paths come first, so an
     * archive staged at the root contributes its entries before anything layered on
     * top of it, and hard link targets are written before the links to them.
     */
    private static List<Path> sources(Path stageDirectory)
            throws IOException
    {
        try (Stream<Path> files = Files.walk(stageDirectory)) {
            return files
                    .filter(Files::isRegularFile)
                    .sorted((left, right) -> {
                        int byDepth = Integer.compare(left.getNameCount(), right.getNameCount());
                        if (byDepth != 0) {
                            return byDepth;
                        }
                        return left.toString().compareTo(right.toString());
                    })
                    .collect(toImmutableList());
        }
    }

    /**
     * Lists plugin archives by name. Their version is stripped when they are staged,
     * so the file name is the artifact id.
     */
    private List<Path> pluginArchives()
            throws IOException
    {
        if (pluginsDirectory == null) {
            return ImmutableList.of();
        }
        try (Stream<Path> files = Files.list(pluginsDirectory)) {
            return files.filter(Files::isRegularFile).sorted().collect(toImmutableList());
        }
    }

    private static String artifactName(Path archive)
    {
        String name = archive.getFileName().toString();
        checkArgument(name.endsWith(".zip"), "Plugin archive is not a zip: %s", archive);
        name = name.substring(0, name.length() - ".zip".length());
        checkArgument(name.startsWith(PLUGIN_PREFIX), "Plugin archive is not named %s<name>.zip: %s", PLUGIN_PREFIX, archive);
        return name;
    }

    private static boolean isArchive(Path file)
    {
        String name = file.getFileName().toString();
        return name.endsWith(".zip") || name.endsWith(".tar.gz");
    }

    private static String relative(Path stageDirectory, Path file)
    {
        return stageDirectory.relativize(file).toString().replace('\\', '/');
    }

    private static String parent(String name)
    {
        int separator = name.lastIndexOf('/');
        if (separator < 0) {
            return "";
        }
        return name.substring(0, separator);
    }

    private static List<PathMatcher> compileGlobs(List<String> globs)
    {
        return globs.stream()
                .map(glob -> FileSystems.getDefault().getPathMatcher("glob:" + glob))
                .collect(toImmutableList());
    }
}
