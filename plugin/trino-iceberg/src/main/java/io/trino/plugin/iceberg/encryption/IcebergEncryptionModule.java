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
package io.trino.plugin.iceberg.encryption;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class IcebergEncryptionModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(IcebergEncryptionConfig.class);
        buildConfigObject(IcebergEncryptionConfig.class).getKmsType().ifPresentOrElse(
                type -> {
                    newOptionalBinder(binder, EncryptionManagerFactory.class)
                            .setDefault().to(DefaultEncryptionManagerFactory.class).in(Scopes.SINGLETON);
                    install(switch (type) {
                        case AWS -> new AwsKmsModule();
                        case AZURE -> new AzureKmsModule();
                        case GCP -> new GcpKmsModule();
                    });
                },
                () -> newOptionalBinder(binder, EncryptionManagerFactory.class)
                        .setDefault().to(PlaintextEncryptionManagerFactory.class).in(Scopes.SINGLETON));
    }
}
