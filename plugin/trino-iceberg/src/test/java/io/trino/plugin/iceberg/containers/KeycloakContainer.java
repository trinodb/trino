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
package io.trino.plugin.iceberg.containers;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.testing.containers.BaseTestContainer;
import org.keycloak.admin.client.Keycloak;
import org.keycloak.admin.client.KeycloakBuilder;
import org.keycloak.admin.client.resource.RealmResource;
import org.keycloak.representations.idm.RealmRepresentation;
import org.testcontainers.containers.Network;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class KeycloakContainer
        extends BaseTestContainer
{
    public static final String DEFAULT_IMAGE = "quay.io/keycloak/keycloak:24.0.1";
    public static final String DEFAULT_HOST_NAME = "keycloak";

    public static final String DEFAULT_USER_NAME = "admin";
    public static final String DEFAULT_PASSWORD = "admin";

    public static final int PORT = 8080;
    public static final String SERVER_URL = "http://" + DEFAULT_HOST_NAME + ":" + PORT;

    public static Builder builder()
    {
        return new Builder();
    }

    private KeycloakContainer(
            String image,
            String hostName,
            Set<Integer> exposePorts,
            Map<String, String> filesToMount,
            Map<String, String> envVars,
            Optional<Network> network,
            int retryLimit)
    {
        super(image, hostName, exposePorts, filesToMount, envVars, network, retryLimit);
    }

    @Override
    protected void setupContainer()
    {
        super.setupContainer();
        withRunCommand(ImmutableList.of("start-dev"));
    }

    @Override
    public void start()
    {
        super.start();
    }

    public String getUrl()
    {
        return "http://" + getMappedHostAndPortForExposedPort(PORT);
    }

    public String getAccessToken()
    {
        String realm = "master";
        String clientId = "admin-cli";

        try (Keycloak keycloak = KeycloakBuilder.builder()
                .serverUrl(getUrl())
                .realm(realm)
                .clientId(clientId)
                .username(DEFAULT_USER_NAME)
                .password(DEFAULT_PASSWORD)
                .build()) {
            RealmResource master = keycloak.realms().realm(realm);
            RealmRepresentation masterRep = master.toRepresentation();
            // change access token lifespan from 1 minute (default) to 1 hour
            // to keep the token alive in case testcase takes more than a minute to finish execution.
            masterRep.setAccessTokenLifespan(3600);
            master.update(masterRep);
            return keycloak.tokenManager().grantToken().getToken();
        }
    }

    public static class Builder
            extends BaseTestContainer.Builder<KeycloakContainer.Builder, KeycloakContainer>
    {
        private Builder()
        {
            this.image = DEFAULT_IMAGE;
            this.hostName = DEFAULT_HOST_NAME;
            this.exposePorts = ImmutableSet.of(PORT);
            this.envVars = ImmutableMap.of(
                    "KEYCLOAK_ADMIN", DEFAULT_USER_NAME,
                    "KEYCLOAK_ADMIN_PASSWORD", DEFAULT_PASSWORD,
                    "KC_HOSTNAME_URL", SERVER_URL);
        }

        @Override
        public KeycloakContainer build()
        {
            return new KeycloakContainer(image, hostName, exposePorts, filesToMount, envVars, network, startupRetryLimit);
        }
    }
}
