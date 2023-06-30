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
package io.trino.server.ui;

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.http.server.HttpServer.ClientCertificate;
import io.trino.server.security.Authenticator;
import io.trino.server.security.CertificateAuthenticator;
import io.trino.server.security.CertificateConfig;
import io.trino.server.security.KerberosAuthenticator;
import io.trino.server.security.KerberosConfig;
import io.trino.server.security.SecurityConfig;
import io.trino.server.security.jwt.JwtAuthenticator;
import io.trino.server.security.jwt.JwtAuthenticatorSupportModule;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.inject.Scopes.SINGLETON;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.configuration.ConfigurationAwareModule.combine;
import static io.airlift.http.server.HttpServer.ClientCertificate.REQUESTED;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class WebUiAuthenticationModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(WebUiAuthenticationConfig.class);

        installWebUiAuthenticator("insecure", new FormUiAuthenticatorModule(false));
        installWebUiAuthenticator("form", new FormUiAuthenticatorModule(true));
        installWebUiAuthenticator("fixed", new FixedUiAuthenticatorModule());
        installWebUiAuthenticator("oauth2", new OAuth2WebUiModule());

        install(webUiAuthenticator("certificate", CertificateAuthenticator.class, certificateBinder -> {
            newOptionalBinder(certificateBinder, ClientCertificate.class).setBinding().toInstance(REQUESTED);
            configBinder(certificateBinder).bindConfig(CertificateConfig.class);
        }));
        installWebUiAuthenticator("kerberos", KerberosAuthenticator.class, KerberosConfig.class);
        install(webUiAuthenticator("jwt", JwtAuthenticator.class, new JwtAuthenticatorSupportModule()));
    }

    private void installWebUiAuthenticator(String type, Module module)
    {
        install(webUiAuthenticator(type, module));
    }

    private void installWebUiAuthenticator(String name, Class<? extends Authenticator> authenticator, Class<?> config)
    {
        install(webUiAuthenticator(name, authenticator, binder -> configBinder(binder).bindConfig(config)));
    }

    public static Module webUiAuthenticator(String type, Module module)
    {
        return new ConditionalWebUiAuthenticationModule(type, module);
    }

    public static Module webUiAuthenticator(String name, Class<? extends Authenticator> clazz, Module module)
    {
        checkArgument(name.toLowerCase(ENGLISH).equals(name), "name is not lower case: %s", name);
        Module authModule = binder -> {
            binder.install(new FormUiAuthenticatorModule(false));
            newOptionalBinder(binder, Key.get(Authenticator.class, ForWebUi.class)).setBinding().to(clazz).in(SINGLETON);
        };
        return webUiAuthenticator(name, combine(module, authModule));
    }

    private static class ConditionalWebUiAuthenticationModule
            extends AbstractConfigurationAwareModule
    {
        private final String type;
        private final Module module;

        public ConditionalWebUiAuthenticationModule(String type, Module module)
        {
            this.type = requireNonNull(type, "type is null");
            this.module = requireNonNull(module, "module is null");
        }

        @Override
        protected void setup(Binder binder)
        {
            if (type.equals(getAuthenticationType())) {
                install(module);
            }
        }

        private String getAuthenticationType()
        {
            String authentication = buildConfigObject(WebUiAuthenticationConfig.class).getAuthentication();
            if (authentication != null) {
                return authentication.toLowerCase(ENGLISH);
            }

            // no authenticator explicitly set for the web ui, so choose a default:
            // If there is a password authenticator, use that.
            List<String> authenticationTypes = buildConfigObject(SecurityConfig.class).getAuthenticationTypes().stream()
                    .map(type -> type.toLowerCase(ENGLISH))
                    .collect(toImmutableList());
            if (authenticationTypes.contains("password")) {
                return "form";
            }
            // otherwise use the first authenticator type
            return authenticationTypes.stream().findFirst().orElseThrow(() -> new IllegalArgumentException("authenticatorTypes is empty"));
        }
    }
}
