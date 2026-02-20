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
 * Combined LDAP and file-authenticator environment used to rerun the base LDAP suite.
 */
public class LdapAndFileRerunEnvironment
        extends LdapBasicEnvironment
{
    @Override
    public String expectedUserNotInGroupMessage(String user)
    {
        return super.expectedUserNotInGroupMessage(user) + " | Access Denied: Invalid credentials";
    }

    @Override
    protected void configureTrino(GenericContainer<?> container)
    {
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

        container.withCopyToContainer(
                Transferable.of(getLdapPasswordAuthenticatorProperties()),
                "/etc/trino/password-authenticator.properties");

        String fileAuthenticator = """
                password-authenticator.name=file
                file.password-file=etc/password.db
                """;
        container.withCopyToContainer(
                Transferable.of(fileAuthenticator),
                "/etc/trino/file-authenticator.properties");

        String passwordDb = """
                DefaultGroupUser:$2y$10$xA36wzOAnGWJmukr/zItyOrOyXPD6prgszOCN93MyFUpMAbGKcklm
                OnlyFileUser:$2y$10$002FmBq75Pos5V5esmi2k.86UpK6BbJRYHt9fpehdj1pJ6xZy5c1S
                """;
        container.withCopyToContainer(
                Transferable.of(passwordDb),
                "/etc/trino/password.db");
    }
}
