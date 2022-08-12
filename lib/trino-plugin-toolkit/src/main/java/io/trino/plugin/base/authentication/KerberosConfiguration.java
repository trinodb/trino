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
package io.trino.plugin.base.authentication;

import com.google.common.collect.ImmutableMap;

import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.nio.file.Files.exists;
import static java.nio.file.Files.isReadable;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public record KerberosConfiguration(KerberosPrincipal kerberosPrincipal, Map<String, String> options)
{
    private static final String KERBEROS_LOGIN_MODULE = "com.sun.security.auth.module.Krb5LoginModule";

    private static final String HOSTNAME_PATTERN = "_HOST";

    public KerberosConfiguration
    {
        requireNonNull(kerberosPrincipal, "kerberosPrincipal is null");
        options = ImmutableMap.copyOf(requireNonNull(options, "options is null"));
    }

    public KerberosConfiguration withDebug()
    {
        ImmutableMap.Builder<String, String> optionsBuilder = ImmutableMap.builder();
        optionsBuilder.putAll(options)
                .put("debug", "true");
        return new KerberosConfiguration(kerberosPrincipal, optionsBuilder.buildOrThrow());
    }

    public Configuration getConfiguration()
    {
        return new Configuration()
        {
            @Override
            public AppConfigurationEntry[] getAppConfigurationEntry(String name)
            {
                return new AppConfigurationEntry[] {
                        new AppConfigurationEntry(
                                KERBEROS_LOGIN_MODULE,
                                AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
                                options)};
            }
        };
    }

    public static class Builder
    {
        private KerberosPrincipal kerberosPrincipal;
        private Optional<String> keytabLocation = Optional.empty();

        public Builder withKerberosPrincipal(String kerberosPrincipal)
        {
            this.kerberosPrincipal = createKerberosPrincipal(kerberosPrincipal);
            return this;
        }

        public Builder withKeytabLocation(String keytabLocation)
        {
            verifyFile(keytabLocation);
            this.keytabLocation = Optional.of(keytabLocation);
            return this;
        }

        public KerberosConfiguration build()
        {
            ImmutableMap.Builder<String, String> optionsBuilder = ImmutableMap.<String, String>builder()
                    .put("doNotPrompt", "true")
                    .put("isInitiator", "true")
                    .put("principal", kerberosPrincipal.getName());

            keytabLocation.ifPresent(
                    keytab -> optionsBuilder
                            .put("storeKey", "true")
                            .put("useKeyTab", "true")
                            .put("keyTab", keytab));

            return new KerberosConfiguration(kerberosPrincipal, optionsBuilder.buildOrThrow());
        }

        private static KerberosPrincipal createKerberosPrincipal(String principal)
        {
            try {
                return new KerberosPrincipal(getServerPrincipal(principal, InetAddress.getLocalHost().getCanonicalHostName()));
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        private static String getServerPrincipal(String principal, String hostname)
        {
            String[] components = principal.split("[/@]");
            if (components.length != 3 || !components[1].equals(HOSTNAME_PATTERN)) {
                return principal;
            }
            return format("%s/%s@%s", components[0], hostname.toLowerCase(ENGLISH), components[2]);
        }

        private static void verifyFile(String fileLocation)
        {
            Path filePath = Paths.get(fileLocation);
            checkArgument(exists(filePath), "File does not exist: %s", fileLocation);
            checkArgument(isReadable(filePath), "File is not readable: %s", fileLocation);
        }
    }
}
