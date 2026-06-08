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

import java.io.IOException;

import static java.util.Objects.requireNonNull;

public class ApplicationDefaultAuth
        implements GcsAuth
{
    private final CredentialsProvider credentialsProvider;

    @Inject
    public ApplicationDefaultAuth()
    {
        this(GoogleCredentials::getApplicationDefault);
    }

    ApplicationDefaultAuth(CredentialsProvider credentialsProvider)
    {
        this.credentialsProvider = requireNonNull(credentialsProvider, "credentialsProvider is null");
    }

    @Override
    public void setAuth(StorageOptions.Builder builder, ConnectorIdentity identity)
    {
        builder.setCredentials(getCredentials());
    }

    private Credentials getCredentials()
    {
        try {
            return credentialsProvider.getCredentials();
        }
        catch (IOException e) {
            return NoCredentials.getInstance();
        }
    }

    @FunctionalInterface
    interface CredentialsProvider
    {
        Credentials getCredentials()
                throws IOException;
    }
}
