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
package io.trino.tests.product.utils;

import java.nio.file.Files;
import java.nio.file.Path;

public final class DownloadedJars
{
    private DownloadedJars() {}

    public static Path downloadedJar(String fileName)
    {
        Path current = Path.of(System.getProperty("user.dir")).toAbsolutePath();
        while (current != null) {
            Path jar = current.resolve("testing/trino-product-tests/target/downloaded-jars/" + fileName);
            if (Files.isRegularFile(jar)) {
                return jar;
            }
            current = current.getParent();
        }
        throw new IllegalStateException(fileName + " was not downloaded; run ./mvnw -pl :trino-product-tests process-test-resources");
    }
}
