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
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.NoCredentials;
import com.google.cloud.storage.StorageOptions;
import com.google.inject.Inject;
import io.trino.spi.security.ConnectorIdentity;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;

public class GcsServiceAccountAuth
        implements GcsAuth
{
    private final Optional<Credentials> jsonGoogleCredential;

    @Inject
    public GcsServiceAccountAuth(GcsFileSystemConfig config)
            throws IOException
    {
        String jsonKey = config.getJsonKey();
        String jsonKeyFilePath = config.getJsonKeyFilePath();

        if (jsonKey != null) {
            try (InputStream inputStream = new ByteArrayInputStream(jsonKey.getBytes(UTF_8))) {
                jsonGoogleCredential = Optional.of(GoogleCredentials.fromStream(inputStream).createScoped(DEFAULT_SCOPES));
            }
        }
        else if (jsonKeyFilePath != null) {
            try (FileInputStream inputStream = new FileInputStream(jsonKeyFilePath)) {
                jsonGoogleCredential = Optional.of(GoogleCredentials.fromStream(inputStream).createScoped(DEFAULT_SCOPES));
            }
        }
        else {
            jsonGoogleCredential = Optional.empty();
        }
    }

    @Override
    public void setAuth(StorageOptions.Builder builder, ConnectorIdentity identity)
    {
        Credentials credentials = jsonGoogleCredential.orElseGet(() -> {
            try {
                return GoogleCredentials.getApplicationDefault();
            }
            catch (IOException e) {
                return NoCredentials.getInstance();
            }
        });

        builder.setCredentials(credentials);
    }
}
