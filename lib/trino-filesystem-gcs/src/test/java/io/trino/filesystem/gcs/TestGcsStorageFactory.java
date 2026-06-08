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
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.NoCredentials;
import com.google.cloud.storage.Storage;
import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.gcs.GcsFileSystemConfig.AuthType;
import io.trino.spi.security.ConnectorIdentity;
import org.junit.jupiter.api.Test;

import static io.trino.filesystem.gcs.GcsFileSystemConstants.EXTRA_CREDENTIALS_GCS_OAUTH_TOKEN_EXPIRES_AT_PROPERTY;
import static io.trino.filesystem.gcs.GcsFileSystemConstants.EXTRA_CREDENTIALS_GCS_OAUTH_TOKEN_PROPERTY;
import static io.trino.filesystem.gcs.GcsFileSystemConstants.EXTRA_CREDENTIALS_GCS_PROJECT_ID_PROPERTY;
import static org.assertj.core.api.Assertions.assertThat;

final class TestGcsStorageFactory
{
    private static final ApplicationDefaultAuth TEST_APPLICATION_DEFAULT_AUTH = new ApplicationDefaultAuth(NoCredentials::getInstance);

    @Test
    void testApplicationDefaultCredentials()
            throws Exception
    {
        GcsFileSystemConfig config = new GcsFileSystemConfig().setAuthType(AuthType.APPLICATION_DEFAULT);
        GcsStorageFactory storageFactory = new GcsStorageFactory(config, TEST_APPLICATION_DEFAULT_AUTH);

        Credentials actualCredentials;
        try (Storage storage = storageFactory.create(ConnectorIdentity.ofUser("test"))) {
            actualCredentials = storage.getOptions().getCredentials();
        }

        assertThat(actualCredentials).isEqualTo(NoCredentials.getInstance());
    }

    @Test
    void testVendedOAuthToken()
            throws Exception
    {
        GcsFileSystemConfig config = new GcsFileSystemConfig().setAuthType(AuthType.APPLICATION_DEFAULT);
        GcsStorageFactory storageFactory = new GcsStorageFactory(config, TEST_APPLICATION_DEFAULT_AUTH);

        ConnectorIdentity identity = ConnectorIdentity.forUser("test")
                .withExtraCredentials(ImmutableMap.of(
                        EXTRA_CREDENTIALS_GCS_OAUTH_TOKEN_PROPERTY, "ya29.test-token"))
                .build();

        try (Storage storage = storageFactory.create(identity)) {
            Credentials credentials = storage.getOptions().getCredentials();
            assertThat(credentials).isInstanceOf(GoogleCredentials.class);
            GoogleCredentials googleCredentials = (GoogleCredentials) credentials;
            AccessToken accessToken = googleCredentials.getAccessToken();
            assertThat(accessToken).isNotNull();
            assertThat(accessToken.getTokenValue()).isEqualTo("ya29.test-token");
        }
    }

    @Test
    void testVendedOAuthTokenWithExpiration()
            throws Exception
    {
        GcsFileSystemConfig config = new GcsFileSystemConfig().setAuthType(AuthType.APPLICATION_DEFAULT);
        GcsStorageFactory storageFactory = new GcsStorageFactory(config, TEST_APPLICATION_DEFAULT_AUTH);

        ConnectorIdentity identity = ConnectorIdentity.forUser("test")
                .withExtraCredentials(ImmutableMap.of(
                        EXTRA_CREDENTIALS_GCS_OAUTH_TOKEN_PROPERTY, "ya29.test-token",
                        EXTRA_CREDENTIALS_GCS_OAUTH_TOKEN_EXPIRES_AT_PROPERTY, "1700000000000"))
                .build();

        try (Storage storage = storageFactory.create(identity)) {
            Credentials credentials = storage.getOptions().getCredentials();
            assertThat(credentials).isInstanceOf(GoogleCredentials.class);
            GoogleCredentials googleCredentials = (GoogleCredentials) credentials;
            AccessToken accessToken = googleCredentials.getAccessToken();
            assertThat(accessToken).isNotNull();
            assertThat(accessToken.getTokenValue()).isEqualTo("ya29.test-token");
            assertThat(accessToken.getExpirationTime()).isNotNull();
            assertThat(accessToken.getExpirationTime().getTime()).isEqualTo(1700000000000L);
        }
    }

    @Test
    void testVendedProjectId()
            throws Exception
    {
        GcsFileSystemConfig config = new GcsFileSystemConfig()
                .setAuthType(AuthType.APPLICATION_DEFAULT)
                .setProjectId("static-project");
        GcsStorageFactory storageFactory = new GcsStorageFactory(config, TEST_APPLICATION_DEFAULT_AUTH);

        ConnectorIdentity identity = ConnectorIdentity.forUser("test")
                .withExtraCredentials(ImmutableMap.of(
                        EXTRA_CREDENTIALS_GCS_OAUTH_TOKEN_PROPERTY, "ya29.test-token",
                        EXTRA_CREDENTIALS_GCS_PROJECT_ID_PROPERTY, "vended-project"))
                .build();

        try (Storage storage = storageFactory.create(identity)) {
            assertThat(storage.getOptions().getProjectId()).isEqualTo("vended-project");
        }
    }

    @Test
    void testStaticConfigUsedWithoutVendedCredentials()
            throws Exception
    {
        GcsFileSystemConfig config = new GcsFileSystemConfig()
                .setAuthType(AuthType.APPLICATION_DEFAULT)
                .setProjectId("static-project");
        GcsStorageFactory storageFactory = new GcsStorageFactory(config, TEST_APPLICATION_DEFAULT_AUTH);

        try (Storage storage = storageFactory.create(ConnectorIdentity.ofUser("test"))) {
            assertThat(storage.getOptions().getProjectId()).isEqualTo("static-project");
            assertThat(storage.getOptions().getCredentials()).isEqualTo(NoCredentials.getInstance());
        }
    }

    @Test
    void testSharedStorageInstanceForDefaultAuth()
    {
        GcsFileSystemConfig config = new GcsFileSystemConfig().setAuthType(AuthType.APPLICATION_DEFAULT);
        GcsStorageFactory storageFactory = new GcsStorageFactory(config, TEST_APPLICATION_DEFAULT_AUTH);

        // Multiple calls without OAuth credentials should return the same cached instance
        Storage storage1 = storageFactory.create(ConnectorIdentity.ofUser("user1"));
        Storage storage2 = storageFactory.create(ConnectorIdentity.ofUser("user2"));

        assertThat(storage1).isSameAs(storage2);

        storageFactory.shutdown();
    }

    @Test
    void testSeparateStorageInstancesForOAuthCredentials()
    {
        GcsFileSystemConfig config = new GcsFileSystemConfig().setAuthType(AuthType.APPLICATION_DEFAULT);
        GcsStorageFactory storageFactory = new GcsStorageFactory(config, TEST_APPLICATION_DEFAULT_AUTH);

        ConnectorIdentity oauthIdentity1 = ConnectorIdentity.forUser("user1")
                .withExtraCredentials(ImmutableMap.of(EXTRA_CREDENTIALS_GCS_OAUTH_TOKEN_PROPERTY, "token1"))
                .build();
        ConnectorIdentity oauthIdentity2 = ConnectorIdentity.forUser("user2")
                .withExtraCredentials(ImmutableMap.of(EXTRA_CREDENTIALS_GCS_OAUTH_TOKEN_PROPERTY, "token2"))
                .build();

        // OAuth requests should get separate instances
        Storage oauthStorage1 = storageFactory.create(oauthIdentity1);
        Storage oauthStorage2 = storageFactory.create(oauthIdentity2);

        assertThat(oauthStorage1).isNotSameAs(oauthStorage2);

        // Default auth should get the shared instance
        Storage defaultStorage = storageFactory.create(ConnectorIdentity.ofUser("default"));
        assertThat(defaultStorage).isNotSameAs(oauthStorage1);
        assertThat(defaultStorage).isNotSameAs(oauthStorage2);

        // Second default auth call should return the same cached instance
        Storage defaultStorage2 = storageFactory.create(ConnectorIdentity.ofUser("default2"));
        assertThat(defaultStorage).isSameAs(defaultStorage2);

        storageFactory.shutdown();
    }
}
