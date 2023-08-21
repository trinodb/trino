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
package io.trino.testing.containers;

import io.airlift.units.DataSize;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.net.UrlEscapers.urlFragmentEscaper;
import static io.airlift.units.DataSize.Unit.GIGABYTE;

public class OpenTracingCollector
        extends BaseTestContainer
{
    private static final int COLLECTOR_PORT = 4317;
    private static final int HTTP_PORT = 16686;

    private final Path storageDirectory;

    public OpenTracingCollector()
    {
        super(
                "jaegertracing/all-in-one:latest",
                "opentracing-collector",
                Set.of(COLLECTOR_PORT, HTTP_PORT),
                Map.of(),
                Map.of(
                    "COLLECTOR_OTLP_ENABLED", "true",
                    "SPAN_STORAGE_TYPE", "badger", // KV that stores spans to the disk
                    "GOMAXPROCS", "2"), // limit number of threads used for goroutines
                Optional.empty(),
                1);

        withRunCommand(List.of(
                "--badger.ephemeral=false",
                "--badger.span-store-ttl=15m",
                "--badger.directory-key=/badger/data",
                "--badger.directory-value=/badger/data",
                "--badger.maintenance-interval=30s"));

        withCreateContainerModifier(command -> command.getHostConfig()
                .withMemory(DataSize.of(1, GIGABYTE).toBytes()));

        try {
            this.storageDirectory = Files.createTempDirectory("tracing-collector");
            mountDirectory(storageDirectory.toString(), "/badger");
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close()
    {
        super.close();
        try (Stream<File> files = Files.walk(storageDirectory)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)) {
            files.forEach(File::delete);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public URI getExporterEndpoint()
    {
        return URI.create("http://" + getMappedHostAndPortForExposedPort(COLLECTOR_PORT));
    }

    public URI searchForQueryId(String queryId)
    {
        String query = "{\"trino.query_id\": \"%s\"}".formatted(queryId);
        return URI.create("http://%s/search?operation=query&service=trino&tags=%s".formatted(getMappedHostAndPortForExposedPort(HTTP_PORT), urlFragmentEscaper().escape(query)));
    }
}
