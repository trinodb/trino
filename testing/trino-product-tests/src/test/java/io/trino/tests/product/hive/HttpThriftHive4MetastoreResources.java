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
package io.trino.tests.product.hive;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

final class HttpThriftHive4MetastoreResources
{
    private static final String RESOURCE_PREFIX = "/hive-http-thrift-metastore/";

    private HttpThriftHive4MetastoreResources() {}

    static String readTextResource(String resourceName)
    {
        String resourcePath = RESOURCE_PREFIX + resourceName;
        try (InputStream inputStream = HttpThriftHive4MetastoreResources.class.getResourceAsStream(resourcePath)) {
            requireNonNull(inputStream, "Missing resource: " + resourcePath);
            return new String(inputStream.readAllBytes(), UTF_8);
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to read resource: " + resourcePath, e);
        }
    }

    static Path extractBinaryResource(String resourceName)
    {
        String resourcePath = RESOURCE_PREFIX + resourceName;
        try (InputStream inputStream = HttpThriftHive4MetastoreResources.class.getResourceAsStream(resourcePath)) {
            requireNonNull(inputStream, "Missing resource: " + resourcePath);
            Path tempFile = Files.createTempFile("hive-http-thrift-metastore-", "-" + Path.of(resourceName).getFileName());
            tempFile.toFile().deleteOnExit();
            Files.write(tempFile, inputStream.readAllBytes());
            return tempFile;
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to extract resource: " + resourcePath, e);
        }
    }
}
