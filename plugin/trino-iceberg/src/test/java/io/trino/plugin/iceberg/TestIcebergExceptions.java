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
package io.trino.plugin.iceberg;

import io.trino.filesystem.TrinoFileSystemException;
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.UnknownHostException;

import static io.trino.plugin.iceberg.IcebergExceptions.translateMetadataException;
import static io.trino.spi.ErrorType.EXTERNAL;
import static org.assertj.core.api.Assertions.assertThat;

class TestIcebergExceptions
{
    @Test
    void testUnresolvableHostIsExternalError()
    {
        // a host name that does not resolve, as the file system reports it out of a metadata read
        assertExternalError(new UncheckedIOException(
                "Failed to open input stream for file: /metadata/00001-00000000-0000-0000-0000-000000000000.metadata.json",
                new IOException("Error fetching properties for file", new UnknownHostException("storage.example.com"))));
    }

    @Test
    void testDeniedMetadataReadIsExternalError()
    {
        // a permission failure, which the file system already marks unrecoverable but leaves without an error code
        assertExternalError(new UncheckedIOException(
                "Failed to open input stream for file: /metadata/00001-00000000-0000-0000-0000-000000000000.metadata.json",
                new IOException(new TrinoFileSystemException("HEAD request failed for file", new RuntimeException("Forbidden (Status Code: 403)")))));
    }

    @Test
    void testFileSystemErrorCodeIsPreserved()
    {
        TrinoException fileSystemFailure = new TrinoException(IcebergErrorCode.ICEBERG_FILESYSTEM_ERROR, "Failed to read file");

        assertThat(translateMetadataException(fileSystemFailure, "schema.table")).isSameAs(fileSystemFailure);
    }

    private static void assertExternalError(Throwable failure)
    {
        RuntimeException translated = translateMetadataException(failure, "schema.materialized_view$materialized_view_storage");

        assertThat(translated).isInstanceOf(TrinoException.class);
        assertThat(((TrinoException) translated).getErrorCode().getType()).isEqualTo(EXTERNAL);
    }
}
