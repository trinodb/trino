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
package io.trino.filesystem.manager;

import com.google.common.collect.ImmutableMap;
import io.opentelemetry.api.OpenTelemetry;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.local.LocalFileSystemFactory;
import io.trino.spi.TrinoException;
import io.trino.spi.security.ConnectorIdentity;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestFileSystemModule
{
    @Test
    void testUnsupportedLocationScheme()
    {
        LocalFileSystemFactory factory = new LocalFileSystemFactory(Path.of("/"));
        TrinoFileSystem fileSystem = FileSystemModule.createFileSystemFactory(
                        new FileSystemConfig(),
                        Optional.empty(),
                        ImmutableMap.of("s3", factory, "gs", factory),
                        Optional.empty(),
                        Optional.empty(),
                        OpenTelemetry.noop().getTracer("test"))
                .create(ConnectorIdentity.ofUser("test"));

        assertThatThrownBy(() -> fileSystem.newInputFile(Location.of("dbfs:/warehouse/table/_delta_log/_last_checkpoint")))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode()))
                .hasMessage("Unsupported file system scheme dbfs for location: dbfs:/warehouse/table/_delta_log/_last_checkpoint. Supported schemes: [s3, gs]");
    }
}
