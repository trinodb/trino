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
package io.trino.filesystem.s3;

import io.airlift.log.Logger;
import io.trino.filesystem.s3.keystore.KeyStoreCredentialAliasResolver;

import java.util.Optional;
import java.util.function.Function;

final class S3KeystoreCredentials
{
    static final String HADOOP_CREDSTORE_PASSWORD_ENVIRONMENT_VARIABLE = "HADOOP_CREDSTORE_PASSWORD";

    private static final Logger log = Logger.get(S3KeystoreCredentials.class);

    private S3KeystoreCredentials() {}

    static Optional<KeyStoreCredentialAliasResolver> createAliasResolver(S3FileSystemConfig config)
    {
        return createAliasResolver(config, System::getenv);
    }

    static Optional<KeyStoreCredentialAliasResolver> createAliasResolver(S3FileSystemConfig config, Function<String, String> environment)
    {
        if (!config.isKeystoreConfigured()) {
            return Optional.empty();
        }

        PasswordResolution passwordResolution = resolveKeystorePassword(config, environment);
        log.info("Using keystore password from %s", passwordResolution.source());

        String entryPassword = Optional.ofNullable(config.getKeystoreEntryPassword())
                .orElse(passwordResolution.password());

        return Optional.of(new KeyStoreCredentialAliasResolver(
                config.getKeystorePath(),
                config.getKeystoreType(),
                passwordResolution.password(),
                entryPassword));
    }

    static PasswordResolution resolveKeystorePassword(S3FileSystemConfig config, Function<String, String> environment)
    {
        if (config.getKeystorePassword() != null) {
            return new PasswordResolution(config.getKeystorePassword(), "s3.keystore.password");
        }
        String environmentPassword = environment.apply(HADOOP_CREDSTORE_PASSWORD_ENVIRONMENT_VARIABLE);
        if (environmentPassword != null) {
            return new PasswordResolution(environmentPassword, HADOOP_CREDSTORE_PASSWORD_ENVIRONMENT_VARIABLE + " environment variable");
        }
        return new PasswordResolution("none", "default ('none')");
    }

    record PasswordResolution(String password, String source) {}
}
