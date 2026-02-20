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
package io.trino.tests.product.ldap;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.Transferable;

/**
 * LDAP environment with both LDAP and file-based authentication.
 * <p>
 * Used for tests with groups: LDAP_AND_FILE, LDAP_AND_FILE_CLI
 * <p>
 * This environment configures Trino to use both LDAP and file password authenticators,
 * allowing users to authenticate either via LDAP or via a password file.
 */
public class LdapAndFileEnvironment
        extends LdapEnvironment
{
    // File authenticator passwords (matching password.db bcrypt hashes)
    private static final String FILE_USER_PASSWORD = "FILEPass123";
    private static final String ONLY_FILE_USER_PASSWORD = "secondFILEPass123";

    @Override
    protected void configureTrino(GenericContainer<?> container)
    {
        // config.properties with multiple authenticators
        String configProperties = """
                node.id=trino-coordinator
                node.environment=test
                coordinator=true
                node-scheduler.include-coordinator=true
                query.max-memory=1GB
                query.max-memory-per-node=1GB
                discovery.uri=http://localhost:8080
                http-server.http.port=8080
                http-server.https.port=8443
                http-server.https.enabled=true
                http-server.https.keystore.path=/etc/trino/certs/trino-server.pem
                http-server.authentication.type=PASSWORD,CERTIFICATE
                password-authenticator.config-files=etc/password-authenticator.properties,etc/file-authenticator.properties
                internal-communication.shared-secret=internal-shared-secret
                catalog.management=dynamic
                """;
        container.withCopyToContainer(
                Transferable.of(configProperties),
                "/etc/trino/config.properties");

        // LDAP password authenticator
        container.withCopyToContainer(
                Transferable.of(getLdapPasswordAuthenticatorProperties()),
                "/etc/trino/password-authenticator.properties");

        // File password authenticator
        String fileAuthenticator = """
                password-authenticator.name=file
                file.password-file=etc/password.db
                """;
        container.withCopyToContainer(
                Transferable.of(fileAuthenticator),
                "/etc/trino/file-authenticator.properties");

        // Password database (bcrypt hashed passwords)
        // DefaultGroupUser:FILEPass123
        // OnlyFileUser:secondFILEPass123
        String passwordDb = """
                DefaultGroupUser:$2y$10$xA36wzOAnGWJmukr/zItyOrOyXPD6prgszOCN93MyFUpMAbGKcklm
                OnlyFileUser:$2y$10$002FmBq75Pos5V5esmi2k.86UpK6BbJRYHt9fpehdj1pJ6xZy5c1S
                """;
        container.withCopyToContainer(
                Transferable.of(passwordDb),
                "/etc/trino/password.db");
    }

    /**
     * Returns the file-based password for the default user (DefaultGroupUser).
     */
    public String getFileUserPassword()
    {
        return FILE_USER_PASSWORD;
    }

    /**
     * Returns the username for the file-only user.
     */
    public String getOnlyFileUser()
    {
        return "OnlyFileUser";
    }

    /**
     * Returns the password for the file-only user.
     */
    public String getOnlyFileUserPassword()
    {
        return ONLY_FILE_USER_PASSWORD;
    }
}
