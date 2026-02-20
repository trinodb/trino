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
package io.trino.tests.product.jdbc;

/**
 * OIDC environment with refresh-token support.
 */
public class JdbcOidcRefreshEnvironment
        extends JdbcOAuth2RefreshEnvironment
{
    @Override
    protected String getBaseConfigProperties()
    {
        return """
                node.environment=test
                coordinator=true
                node-scheduler.include-coordinator=true
                http-server.http.port=8080
                query.max-memory=2GB
                query.max-memory-per-node=1.25GB
                discovery.uri=http://localhost:8080
                web-ui.enabled=true
                http-server.authentication.type=oauth2
                http-server.https.port=%d
                http-server.https.enabled=true
                http-server.https.keystore.path=/etc/trino/trino.pem
                http-server.authentication.oauth2.issuer=https://hydra:4444/
                http-server.authentication.oauth2.client-id=%s
                http-server.authentication.oauth2.client-secret=%s
                http-server.authentication.oauth2.user-mapping.pattern=(.*)(@.*)?
                http-server.authentication.oauth2.oidc.use-userinfo-endpoint=false
                oauth2-jwk.http-client.trust-store-path=/etc/trino/hydra.pem
                internal-communication.shared-secret=internal-shared-secret
                catalog.management=dynamic
                query.min-expire-age=1m
                task.info.max-age=1m
                """.formatted(HTTPS_PORT, CLIENT_ID, CLIENT_SECRET);
    }
}
