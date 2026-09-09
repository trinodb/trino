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

import io.opentelemetry.api.OpenTelemetry;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.spi.TrinoException;
import io.trino.spi.security.ConnectorIdentity;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestFileSystemModule
{
    @Test
    public void testMissingS3Factory()
    {
        TrinoFileSystemFactory fileSystemFactory = createFileSystemFactory(Map.of());
        Location location = Location.of("s3://bucket/path");

        assertThatThrownBy(() -> fileSystemFactory.create(ConnectorIdentity.ofUser("test")).newInputFile(location))
                .isInstanceOf(TrinoException.class)
                .matches(throwable -> ((TrinoException) throwable).getErrorCode().equals(CONFIGURATION_INVALID.toErrorCode()))
                .hasMessageContaining("fs.s3.enabled");
    }

    @Test
    public void testS3FactoryResolvesWhenRegistered()
    {
        TrinoFileSystemFactory s3Factory = _ -> {
            throw new UnsupportedOperationException("resolved s3 factory");
        };
        TrinoFileSystemFactory fileSystemFactory = createFileSystemFactory(Map.of("s3", s3Factory));
        Location location = Location.of("s3://bucket/path");

        assertThatThrownBy(() -> fileSystemFactory.create(ConnectorIdentity.ofUser("test")).newInputFile(location))
                .isInstanceOf(UnsupportedOperationException.class)
                .hasMessage("resolved s3 factory");
    }

    @Test
    public void testMissingGcsFactory()
    {
        TrinoFileSystemFactory fileSystemFactory = createFileSystemFactory(Map.of());
        Location location = Location.of("gs://bucket/path");

        assertThatThrownBy(() -> fileSystemFactory.create(ConnectorIdentity.ofUser("test")).newInputFile(location))
                .isInstanceOf(TrinoException.class)
                .matches(throwable -> ((TrinoException) throwable).getErrorCode().equals(CONFIGURATION_INVALID.toErrorCode()))
                .hasMessageContaining("fs.gcs.enabled");
    }

    @Test
    public void testUnknownSchemeUsesGenericMessage()
    {
        TrinoFileSystemFactory fileSystemFactory = createFileSystemFactory(Map.of());
        Location location = Location.of("unknown://bucket/path");

        assertThatThrownBy(() -> fileSystemFactory.create(ConnectorIdentity.ofUser("test")).newInputFile(location))
                .isInstanceOf(TrinoException.class)
                .matches(throwable -> ((TrinoException) throwable).getErrorCode().equals(CONFIGURATION_INVALID.toErrorCode()))
                .hasMessageContaining("unknown://bucket/path")
                .matches(throwable -> !throwable.getMessage().contains("fs.s3.enabled"));
    }

    private static TrinoFileSystemFactory createFileSystemFactory(Map<String, TrinoFileSystemFactory> factories)
    {
        return FileSystemModule.createFileSystemFactory(
                new FileSystemConfig(),
                Optional.empty(),
                factories,
                Optional.empty(),
                Optional.empty(),
                OpenTelemetry.noop().getTracer("test"));
    }
}
