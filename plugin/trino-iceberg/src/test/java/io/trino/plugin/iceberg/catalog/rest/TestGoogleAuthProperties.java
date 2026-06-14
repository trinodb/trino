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
package io.trino.plugin.iceberg.catalog.rest;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.apache.iceberg.gcp.auth.GoogleAuthManager.GCP_CREDENTIALS_PATH_PROPERTY;
import static org.apache.iceberg.rest.auth.AuthProperties.AUTH_TYPE;
import static org.apache.iceberg.rest.auth.AuthProperties.AUTH_TYPE_GOOGLE;
import static org.assertj.core.api.Assertions.assertThat;

final class TestGoogleAuthProperties
{
    @Test
    void testApplicationDefaultCredentials()
    {
        GoogleSecurityConfig config = new GoogleSecurityConfig()
                .setProjectId("my-project");
        // jsonKeyFilePath intentionally omitted to use Workload Identity / ADC

        Map<String, String> properties = new GoogleAuthProperties(config).get();

        assertThat(properties).containsEntry(AUTH_TYPE, AUTH_TYPE_GOOGLE);
        assertThat(properties).containsEntry("header.x-goog-user-project", "my-project");
        assertThat(properties).doesNotContainKey(GCP_CREDENTIALS_PATH_PROPERTY);
    }

    @Test
    void testServiceAccountJsonKeyFile(@TempDir Path tempDir)
            throws IOException
    {
        Path keyFile = Files.createFile(tempDir.resolve("key.json"));
        GoogleSecurityConfig config = new GoogleSecurityConfig()
                .setProjectId("my-project")
                .setJsonKeyFilePath(keyFile.toString());

        Map<String, String> properties = new GoogleAuthProperties(config).get();

        assertThat(properties).containsEntry(AUTH_TYPE, AUTH_TYPE_GOOGLE);
        assertThat(properties).containsEntry("header.x-goog-user-project", "my-project");
        assertThat(properties).containsEntry(GCP_CREDENTIALS_PATH_PROPERTY, keyFile.toString());
    }
}
