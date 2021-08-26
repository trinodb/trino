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
package io.trino.plugin.elasticsearch;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.plugin.elasticsearch.client.ElasticsearchClient;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConditionalModule.conditionalModule;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.elasticsearch.ElasticsearchConfig.Security.AWS;
import static io.trino.plugin.elasticsearch.ElasticsearchConfig.Security.PASSWORD;
import static java.util.function.Predicate.isEqual;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

public class ElasticsearchConnectorModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        binder.bind(ElasticsearchConnector.class).in(Scopes.SINGLETON);
        binder.bind(ElasticsearchMetadata.class).in(Scopes.SINGLETON);
        binder.bind(ElasticsearchSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(ElasticsearchPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(ElasticsearchClient.class).in(Scopes.SINGLETON);
        binder.bind(NodesSystemTable.class).in(Scopes.SINGLETON);

        newExporter(binder).export(ElasticsearchClient.class).withGeneratedName();

        configBinder(binder).bindConfig(ElasticsearchConfig.class);

        newOptionalBinder(binder, AwsSecurityConfig.class);
        newOptionalBinder(binder, PasswordConfig.class);

        install(conditionalModule(
                ElasticsearchConfig.class,
                config -> config.getSecurity()
                        .filter(isEqual(AWS))
                        .isPresent(),
                conditionalBinder -> configBinder(conditionalBinder).bindConfig(AwsSecurityConfig.class)));

        install(conditionalModule(
                ElasticsearchConfig.class,
                config -> config.getSecurity()
                        .filter(isEqual(PASSWORD))
                        .isPresent(),
                conditionalBinder -> configBinder(conditionalBinder).bindConfig(PasswordConfig.class)));
    }
}
