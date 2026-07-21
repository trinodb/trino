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
package io.trino.plugin.iceberg.procedure;

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.spi.TrinoException;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;
import java.util.regex.Pattern;

import static io.trino.plugin.base.util.Procedures.checkProcedureArgument;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_FILESYSTEM_ERROR;
import static io.trino.plugin.iceberg.IcebergUtil.METADATA_FOLDER_NAME;
import static io.trino.plugin.iceberg.IcebergUtil.getLatestMetadataLocation;
import static io.trino.spi.StandardErrorCode.INVALID_PROCEDURE_ARGUMENT;
import static java.lang.String.format;
import static org.apache.iceberg.util.LocationUtil.stripTrailingSlash;

final class RegisterProcedureUtils
{
    private static final Pattern S3_SCHEMA_PATTERN = Pattern.compile("^s3[an]://");

    private RegisterProcedureUtils() {}

    static void validateMetadataFileName(String fileName)
    {
        String metadataFileName = fileName.trim();
        checkProcedureArgument(!metadataFileName.isEmpty(), "metadata_file_name cannot be empty when provided as an argument");
        checkProcedureArgument(!metadataFileName.contains("/"), "%s is not a valid metadata file", metadataFileName);
    }

    /**
     * Get the latest metadata file location present in {@code location} if {@code metadataFileName} is not provided,
     * otherwise form the metadata file location using {@code location} and {@code metadataFileName}.
     */
    static String getMetadataLocation(TrinoFileSystem fileSystem, String location, Optional<String> metadataFileName)
    {
        return metadataFileName
                .map(fileName -> format("%s/%s/%s", stripTrailingSlash(location), METADATA_FOLDER_NAME, fileName))
                .orElseGet(() -> getLatestMetadataLocation(fileSystem, location));
    }

    static void validateMetadataLocation(TrinoFileSystem fileSystem, Location location)
    {
        try {
            if (!fileSystem.newInputFile(location).exists()) {
                throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, "Metadata file does not exist: " + location);
            }
        }
        catch (IOException | UncheckedIOException e) {
            throw new TrinoException(ICEBERG_FILESYSTEM_ERROR, "Invalid metadata file location: " + location, e);
        }
    }

    static boolean locationEquivalent(String a, String b)
    {
        return normalizeS3Uri(a).equals(normalizeS3Uri(b));
    }

    private static String normalizeS3Uri(String location)
    {
        // Normalize e.g. s3a to s3, so that a table or view can be registered using an s3:// location
        // even if internally it uses s3a:// paths.
        String normalizedSchema = S3_SCHEMA_PATTERN.matcher(location).replaceFirst("s3://");
        // Remove trailing slashes so that test_dir is equal to test_dir/
        return stripTrailingSlash(normalizedSchema);
    }
}
