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

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.StorageOptions;
import io.trino.spi.security.ConnectorIdentity;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static com.google.common.base.Strings.nullToEmpty;
import static io.trino.filesystem.gcs.GcsStorageFactory.GCS_OAUTH_KEY;
import static java.nio.charset.StandardCharsets.UTF_8;

public class GcsAccessTokenAuth
        implements GcsAuth
{
    @Override
    public void setAuth(StorageOptions.Builder builder, ConnectorIdentity identity)
            throws IOException
    {
        String accessToken = nullToEmpty(identity.getExtraCredentials().get(GCS_OAUTH_KEY));
        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(accessToken.getBytes(UTF_8))) {
            GoogleCredentials credentials = GoogleCredentials.fromStream(inputStream).createScoped(DEFAULT_SCOPES);
            if (credentials != null) {
                builder.setCredentials(credentials);
            }
        }
    }
}
