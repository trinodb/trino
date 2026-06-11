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
package io.trino.filesystem.gcs;

import io.trino.filesystem.encryption.EncryptionKey;
import org.junit.jupiter.api.BeforeAll;

import java.io.IOException;

import static io.trino.filesystem.encryption.EncryptionKey.randomAes256;
import static io.trino.filesystem.gcs.GcsUtils.encodedKey;
import static io.trino.testing.SystemEnvironmentUtils.requireEnv;

final class TestGcsFileSystemWithConfiguredEncryptionKey
        extends AbstractTestGcsFileSystem
{
    private final EncryptionKey encryptionKey = randomAes256();

    @BeforeAll
    void setup()
            throws IOException
    {
        initialize(requireEnv("GCP_CREDENTIALS_KEY"), new GcsFileSystemConfig()
                .setEncryptionKey(encodedKey(encryptionKey))
                .setDecryptionKey(encodedKey(encryptionKey)));
    }
}
