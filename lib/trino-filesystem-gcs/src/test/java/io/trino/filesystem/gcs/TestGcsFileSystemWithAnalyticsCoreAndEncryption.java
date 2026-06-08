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

import org.junit.jupiter.api.BeforeAll;

import java.io.IOException;
import java.util.Optional;

import static io.trino.filesystem.gcs.GcsUtils.encodedKey;

public class TestGcsFileSystemWithAnalyticsCoreAndEncryption
        extends TestGcsFileSystemWithAnalyticsCore
{
    @Override
    @BeforeAll
    void setup()
            throws IOException
    {
        String staticBucket = System.getenv("GCS_TEST_BUCKET");
        String gcpCredentialsKey = System.getenv("GCP_CREDENTIALS_KEY");

        if (gcpCredentialsKey == null || gcpCredentialsKey.isBlank()) {
            initializeWithApplicationDefault(staticBucket, config -> config
                    .setAnalyticsCoreEnabled(true)
                    .setDecryptionKey(Optional.of(encodedKey(randomEncryptionKey))));
            return;
        }

        initialize(gcpCredentialsKey, staticBucket, config -> config
                .setAnalyticsCoreEnabled(true)
                .setDecryptionKey(Optional.of(encodedKey(randomEncryptionKey))));
    }

    @Override
    protected boolean useServerSideEncryptionWithCustomerKey()
    {
        return true;
    }

    protected boolean encryptionDelegateHasDecryptionKey()
    {
        // With Analytics Core, the decryption key is configured globally in GcsReadOptions,
        // so the delegate file system also has access to the key for decryption
        return true;
    }
}
