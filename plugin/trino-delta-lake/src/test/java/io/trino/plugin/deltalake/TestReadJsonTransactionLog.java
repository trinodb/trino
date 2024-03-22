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
package io.trino.plugin.deltalake;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.airlift.json.ObjectMapperProvider;
import io.trino.plugin.deltalake.transactionlog.AddFileEntry;
import io.trino.plugin.deltalake.transactionlog.DeltaLakeTransactionLogEntry;
import io.trino.plugin.deltalake.transactionlog.RemoveFileEntry;
import io.trino.plugin.deltalake.transactionlog.checkpoint.LastCheckpoint;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Stream;

import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.assertj.core.api.Assertions.assertThat;

public class TestReadJsonTransactionLog
{
    private final ObjectMapper objectMapper = new ObjectMapperProvider().get();

    @Test
    public void testAdd()
    {
        assertThat(readJsonTransactionLogs("databricks73/person/_delta_log")
                .map(this::deserialize)
                .map(DeltaLakeTransactionLogEntry::getAdd)
                .filter(Objects::nonNull)
                .map(AddFileEntry::getPath)
                .filter(Objects::nonNull)
                .count()).isEqualTo(18);

        assertThat(readJsonTransactionLogs("deltalake/person/_delta_log")
                .map(this::deserialize)
                .map(DeltaLakeTransactionLogEntry::getAdd)
                .filter(Objects::nonNull)
                .map(AddFileEntry::getPath)
                .filter(Objects::nonNull)
                .count()).isEqualTo(18);
    }

    @Test
    public void testRemove()
    {
        assertThat(readJsonTransactionLogs("databricks73/person/_delta_log")
                .map(this::deserialize)
                .map(DeltaLakeTransactionLogEntry::getRemove)
                .filter(Objects::nonNull)
                .map(RemoveFileEntry::path)
                .filter(Objects::nonNull)
                .count()).isEqualTo(6);

        assertThat(readJsonTransactionLogs("deltalake/person/_delta_log")
                .map(this::deserialize)
                .map(DeltaLakeTransactionLogEntry::getRemove)
                .filter(Objects::nonNull)
                .map(RemoveFileEntry::path)
                .filter(Objects::nonNull)
                .count()).isEqualTo(6);
    }

    @Test
    public void testReadLastCheckpointFile()
            throws JsonProcessingException
    {
        LastCheckpoint lastCheckpoint = objectMapper.readValue("{\"version\":10,\"size\":17}", LastCheckpoint.class);
        assertThat(lastCheckpoint.version()).isEqualTo(10L);
        assertThat(lastCheckpoint.size()).isEqualTo(17);
        assertThat(lastCheckpoint.parts()).isEmpty();
    }

    @Test
    public void testReadLastCheckpointFileForMultipart()
            throws JsonProcessingException
    {
        LastCheckpoint lastCheckpoint = objectMapper.readValue("{\"version\":237580,\"size\":658573,\"parts\":2}", LastCheckpoint.class);
        assertThat(lastCheckpoint.version()).isEqualTo(237580L);
        assertThat(lastCheckpoint.size()).isEqualTo(658573L);
        assertThat(lastCheckpoint.parts()).hasValue(2);
    }

    private Stream<String> readJsonTransactionLogs(String location)
    {
        File directory = directoryForResource(location);
        File[] files = directory.listFiles((dir, name) -> name.matches("[0-9]{20}\\.json"));
        verify(files != null);
        return Arrays.stream(files)
                .sorted()
                .flatMap(TestReadJsonTransactionLog::lines)
                // lines are json strings followed by 'x' in the Databricks version of Delta
                .map(line -> line.endsWith("x") ? line.substring(0, line.length() - 1) : line);
    }

    private static Stream<String> lines(File file)
    {
        try (Stream<String> lines = Files.lines(file.toPath())) {
            return lines
                    .collect(toImmutableList())
                    .stream();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private DeltaLakeTransactionLogEntry deserialize(String json)
    {
        try {
            return objectMapper.readValue(json, DeltaLakeTransactionLogEntry.class);
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to parse " + json, e);
        }
    }

    private File directoryForResource(String location)
    {
        try {
            return new File(getClass().getClassLoader().getResource(location).toURI());
        }
        catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }
}
