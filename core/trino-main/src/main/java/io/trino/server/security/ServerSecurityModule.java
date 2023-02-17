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
package io.trino.server.security;

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.multibindings.MapBinder;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.discovery.server.DynamicAnnouncementResource;
import io.airlift.discovery.server.ServiceResource;
import io.airlift.discovery.store.StoreResource;
import io.airlift.http.server.HttpServer.ClientCertificate;
import io.airlift.http.server.HttpServerConfig;
import io.airlift.jmx.MBeanResource;
import io.airlift.openmetrics.MetricsResource;
import io.trino.server.security.jwt.JwtAuthenticator;
import io.trino.server.security.jwt.JwtAuthenticatorSupportModule;
import io.trino.server.security.oauth2.OAuth2AuthenticationSupportModule;
import io.trino.server.security.oauth2.OAuth2Authenticator;
import io.trino.server.security.oauth2.OAuth2Client;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.configuration.ConfigurationAwareModule.combine;
import static io.airlift.http.server.HttpServer.ClientCertificate.REQUESTED;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static io.trino.server.security.ResourceSecurityBinder.resourceSecurityBinder;
import static java.util.Locale.ENGLISH;

public class ServerSecurityModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        binder.bind(AuthenticationFilter.class);
        jaxrsBinder(binder).bind(ResourceSecurityDynamicFeature.class);

        resourceSecurityBinder(binder)
                .managementReadResource(ServiceResource.class)
                .managementReadResource(MBeanResource.class)
                .managementReadResource(MetricsResource.class)
                .internalOnlyResource(DynamicAnnouncementResource.class)
                .internalOnlyResource(StoreResource.class);

        newOptionalBinder(binder, PasswordAuthenticatorManager.class);
        binder.bind(CertificateAuthenticatorManager.class).in(Scopes.SINGLETON);
        newOptionalBinder(binder, HeaderAuthenticatorManager.class);
        insecureHttpAuthenticationDefaults();

        authenticatorBinder(binder); // create empty map binder

        install(authenticatorModule("certificate", CertificateAuthenticator.class, certificateBinder -> {
            newOptionalBinder(certificateBinder, ClientCertificate.class).setBinding().toInstance(REQUESTED);
            configBinder(certificateBinder).bindConfig(CertificateConfig.class);
        }));
        installAuthenticator("kerberos", KerberosAuthenticator.class, KerberosConfig.class);
        install(authenticatorModule("password", PasswordAuthenticator.class, used -> {
            configBinder(binder).bindConfig(PasswordAuthenticatorConfig.class);
            binder.bind(PasswordAuthenticatorManager.class).in(Scopes.SINGLETON);
        }));
        install(authenticatorModule("header", HeaderAuthenticator.class, headerBinder -> {
            configBinder(headerBinder).bindConfig(HeaderAuthenticatorConfig.class);
            headerBinder.bind(HeaderAuthenticatorManager.class).in(Scopes.SINGLETON);
        }));
        install(authenticatorModule("jwt", JwtAuthenticator.class, new JwtAuthenticatorSupportModule()));
        install(authenticatorModule("oauth2", OAuth2Authenticator.class, new OAuth2AuthenticationSupportModule()));
        newOptionalBinder(binder, OAuth2Client.class);

        configBinder(binder).bindConfig(InsecureAuthenticatorConfig.class);
        binder.bind(InsecureAuthenticator.class).in(Scopes.SINGLETON);
        install(authenticatorModule("insecure", InsecureAuthenticator.class, unused -> {}));
    }

    @Provides
    public List<Authenticator> getAuthenticatorList(SecurityConfig config, Map<String, Authenticator> authenticators)
    {
        return authenticationTypes(config).stream()
                .map(type -> {
                    Authenticator authenticator = authenticators.get(type);
                    if (authenticator == null) {
                        throw new RuntimeException("Unknown authenticator type: " + type);
                    }
                    return authenticator;
                })
                .collect(toImmutableList());
    }

    public static Module authenticatorModule(String name, Class<? extends Authenticator> clazz, Module module)
    {
        checkArgument(name.toLowerCase(ENGLISH).equals(name), "name is not lower case: %s", name);
        Module authModule = binder -> authenticatorBinder(binder).addBinding(name).to(clazz).in(Scopes.SINGLETON);
        return conditionalModule(
                SecurityConfig.class,
                config -> authenticationTypes(config).contains(name),
                combine(module, authModule));
    }

    private void installAuthenticator(String name, Class<? extends Authenticator> authenticator, Class<?> config)
    {
        install(authenticatorModule(name, authenticator, binder -> configBinder(binder).bindConfig(config)));
    }

    private static MapBinder<String, Authenticator> authenticatorBinder(Binder binder)
    {
        return newMapBinder(binder, String.class, Authenticator.class);
    }

    private static List<String> authenticationTypes(SecurityConfig config)
    {
        return config.getAuthenticationTypes().stream()
                .map(type -> type.toLowerCase(ENGLISH))
                .collect(toImmutableList());
    }

    private void insecureHttpAuthenticationDefaults()
    {
        HttpServerConfig httpServerConfig = buildConfigObject(HttpServerConfig.class);
        SecurityConfig securityConfig = buildConfigObject(SecurityConfig.class);
        // if secure https authentication is enabled, disable insecure authentication over http
        if ((httpServerConfig.isHttpsEnabled() || httpServerConfig.isProcessForwarded()) &&
                !securityConfig.getAuthenticationTypes().equals(ImmutableList.of("insecure"))) {
            install(binder -> configBinder(binder).bindConfigDefaults(SecurityConfig.class, config -> config.setInsecureAuthenticationOverHttpAllowed(false)));
        }
    }
}
