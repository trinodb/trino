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
package io.trino.metadata;

import com.google.common.util.concurrent.UncheckedExecutionException;
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;

import static io.trino.metadata.MetadataListing.handleListingException;
import static io.trino.spi.StandardErrorCode.GENERIC_EXTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.StandardErrorCode.TABLE_NOT_FOUND;
import static org.assertj.core.api.Assertions.assertThat;

public class TestMetadataListing
{
    @Test
    public void testUnclassifiedFailureIsExternal()
    {
        TrinoException exception = handleListingException(
                new IllegalArgumentException("Unsupported column type"),
                "table columns",
                "test_catalog");

        assertThat(exception.getErrorCode()).isEqualTo(GENERIC_EXTERNAL_ERROR.toErrorCode());
        assertThat(exception.getMessage()).isEqualTo("Error listing table columns for catalog test_catalog: Unsupported column type");
    }

    @Test
    public void testWrappedUnclassifiedFailureIsExternal()
    {
        TrinoException exception = handleListingException(
                new UncheckedExecutionException(new RuntimeException("Failed to connect SSH tunnel", new Exception("Connection refused"))),
                "tables",
                "test_catalog");

        assertThat(exception.getErrorCode()).isEqualTo(GENERIC_EXTERNAL_ERROR.toErrorCode());
        // The wrapper carries the message of its cause, so the reason for the failure stays visible
        assertThat(exception.getMessage()).isEqualTo("Error listing tables for catalog test_catalog: java.lang.RuntimeException: Failed to connect SSH tunnel");
    }

    @Test
    public void testErrorCodeIsPreserved()
    {
        TrinoException exception = handleListingException(
                new TrinoException(TABLE_NOT_FOUND, "Table not found"),
                "tables",
                "test_catalog");

        assertThat(exception.getErrorCode()).isEqualTo(TABLE_NOT_FOUND.toErrorCode());
    }

    @Test
    public void testWrappedErrorCodeIsPreserved()
    {
        TrinoException exception = handleListingException(
                new UncheckedExecutionException(new TrinoException(GENERIC_INTERNAL_ERROR, "Metastore is unreachable")),
                "tables",
                "test_catalog");

        assertThat(exception.getErrorCode()).isEqualTo(GENERIC_INTERNAL_ERROR.toErrorCode());
    }
}
