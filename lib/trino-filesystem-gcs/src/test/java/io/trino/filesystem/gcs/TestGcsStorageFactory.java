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

import com.google.auth.Credentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import io.trino.spi.security.ConnectorIdentity;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

final class TestGcsStorageFactory
{
    @Test
    void testDefaultCredentials()
            throws Exception
    {
        Credentials expectedCredentials = StorageOptions.newBuilder().build().getCredentials();

        // No credentials options are set
        GcsFileSystemConfig config = new GcsFileSystemConfig();

        GcsStorageFactory storageFactory = new GcsStorageFactory(config);

        Credentials actualCredentials;
        try (Storage storage = storageFactory.create(ConnectorIdentity.ofUser("test"))) {
            actualCredentials = storage.getOptions().getCredentials();
        }

        assertThat(actualCredentials)
                .as("if credentials are not explicitly configured, should have same behavior as the GCS client")
                .isEqualTo(expectedCredentials);
    }
}
