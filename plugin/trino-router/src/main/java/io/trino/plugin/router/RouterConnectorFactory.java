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
package io.trino.plugin.router;

import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.log.Logger;
import io.trino.plugin.base.ConnectorContextModule;
import io.trino.plugin.base.jmx.MBeanServerModule;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.GlueClientBuilder;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.StsClientBuilder;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.services.sts.auth.StsWebIdentityTokenFileCredentialsProvider;

import java.util.Map;
import java.util.Optional;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class RouterConnectorFactory
        implements ConnectorFactory
{
    private static final Logger log = Logger.get(RouterConnectorFactory.class);

    @Override
    public String getName()
    {
        return "router";
    }

    @Override
    public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
    {
        log.info("Creating router connector for catalog '%s'", catalogName);
        // No checkStrictSpiVersionMatch: that guard is intended only for plugins distributed with Trino.
        // trino-router is an EG-internal plugin deployed as a standalone ZIP across OS Trino and Starburst
        // (e.g. 476-e.7), whose SPI version strings never match exactly. The SPI surface used here is stable
        // across these versions, so a single artifact is binary-compatible without the exact-version check.
        Bootstrap app = new Bootstrap(
                "io.trino.bootstrap.catalog." + catalogName,
                new MBeanServerModule(),
                new ConnectorContextModule(catalogName, context),
                new RouterModule());

        Injector injector = app
                .doNotInitializeLogging()
                .disableSystemProperties()
                .setRequiredConfigurationProperties(config)
                .initialize();

        return injector.getInstance(RouterConnector.class);
    }

    private static class RouterModule
            extends AbstractModule
    {
        @Override
        protected void configure()
        {
            configBinder(binder()).bindConfig(RouterConfig.class);
            bind(RouterConnector.class).in(Scopes.SINGLETON);
            bind(RouterMetadata.class).in(Scopes.SINGLETON);
        }

        @Provides
        @Singleton
        public GlueClient provideGlueClient(RouterConfig routerConfig)
        {
            GlueClientBuilder glue = GlueClient.builder();

            Optional<StaticCredentialsProvider> staticCredentials = Optional.empty();
            if (routerConfig.getGlueAwsAccessKey().isPresent() && routerConfig.getGlueAwsSecretKey().isPresent()) {
                staticCredentials = Optional.of(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(routerConfig.getGlueAwsAccessKey().get(), routerConfig.getGlueAwsSecretKey().get())));
            }

            if (routerConfig.isGlueUseWebIdentityTokenCredentialsProvider()) {
                log.info("Router Glue auth: web identity token credentials provider (configured region=%s)", routerConfig.getGlueRegion().orElse("<default>"));
                StsClientBuilder sts = StsClient.builder();
                staticCredentials.ifPresent(sts::credentialsProvider);
                routerConfig.getGlueRegion().ifPresent(r -> sts.region(Region.of(r)));
                glue.credentialsProvider(StsWebIdentityTokenFileCredentialsProvider.builder()
                        .stsClient(sts.build())
                        .asyncCredentialUpdateEnabled(true)
                        .build());
            }
            else if (routerConfig.getGlueIamRole().isPresent()) {
                log.info("Router Glue auth: assume IAM role %s (externalId set=%s, configured region=%s)",
                        routerConfig.getGlueIamRole().get(),
                        routerConfig.getGlueExternalId().isPresent(),
                        routerConfig.getGlueRegion().orElse("<default>"));
                StsClientBuilder sts = StsClient.builder();
                staticCredentials.ifPresent(sts::credentialsProvider);
                routerConfig.getGlueRegion().ifPresent(r -> sts.region(Region.of(r)));
                glue.credentialsProvider(StsAssumeRoleCredentialsProvider.builder()
                        .refreshRequest(request -> request
                                .roleArn(routerConfig.getGlueIamRole().get())
                                .roleSessionName("trino-router-session")
                                .externalId(routerConfig.getGlueExternalId().orElse(null)))
                        .stsClient(sts.build())
                        .asyncCredentialUpdateEnabled(true)
                        .build());
            }
            else {
                log.info("Router Glue auth: %s credentials (configured region=%s)",
                        staticCredentials.isPresent() ? "static" : "default provider chain",
                        routerConfig.getGlueRegion().orElse("<default>"));
                staticCredentials.ifPresent(glue::credentialsProvider);
            }

            if (routerConfig.getGlueRegion().isPresent()) {
                glue.region(Region.of(routerConfig.getGlueRegion().get()));
            }
            else if (routerConfig.isGluePinClientToCurrentRegion()) {
                Region resolvedRegion = DefaultAwsRegionProviderChain.builder().build().getRegion();
                log.info("Router Glue client pinned to current region %s", resolvedRegion);
                glue.region(resolvedRegion);
            }

            glue.httpClientBuilder(ApacheHttpClient.builder());

            return glue.build();
        }
    }
}
