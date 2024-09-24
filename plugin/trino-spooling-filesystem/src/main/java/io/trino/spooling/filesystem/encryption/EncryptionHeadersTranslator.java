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
package io.trino.spooling.filesystem.encryption;

import io.trino.filesystem.Location;
import io.trino.filesystem.encryption.EncryptionKey;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public interface EncryptionHeadersTranslator
{
    EncryptionKey extractKey(Map<String, List<String>> headers);

    Map<String, List<String>> createHeaders(EncryptionKey encryption);

    static EncryptionHeadersTranslator encryptionHeadersTranslator(Location location)
    {
        requireNonNull(location, "location is null");
        return location.scheme()
                .map(EncryptionHeadersTranslator::forScheme)
                .orElseThrow(() -> new IllegalArgumentException("Unknown location scheme: " + location));
    }

    private static EncryptionHeadersTranslator forScheme(String scheme)
    {
        // These should match schemes supported in the FileSystemSpoolingModule
        return switch (scheme) {
            case "s3" -> new S3EncryptionHeadersTranslator();
            case "gs" -> new GcsEncryptionHeadersTranslator();
            case "abfs" -> new AzureEncryptionHeadersTranslator();
            default -> throw new IllegalArgumentException("Unknown file system scheme: " + scheme);
        };
    }
}
