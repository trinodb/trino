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
package io.trino.plugin.iceberg.catalog.rest;

import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.trino.filesystem.s3.S3FileSystemConfig;

import java.lang.reflect.Constructor;

import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;

public class SigV4SecurityModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(IcebergRestCatalogSigV4Config.class);
        binder.bind(SecurityProperties.class).to(SigV4AwsProperties.class);
        IcebergRestCatalogSigV4Config sigV4Config = buildConfigObject(IcebergRestCatalogSigV4Config.class);
        if (sigV4Config.isStsWebIdentity()) {
            // Use toConstructor to avoid the Guice cycle: OptionalBinder internally creates
            // T → @Actual T, so .to(T.class) would produce T → @Actual T → T.
            try {
                Constructor<OidcStsCredentialExchanger> ctor = OidcStsCredentialExchanger.class
                        .getConstructor(S3FileSystemConfig.class, IcebergRestCatalogSigV4Config.class);
                newOptionalBinder(binder, OidcStsCredentialExchanger.class)
                        .setBinding()
                        .toConstructor(ctor)
                        .in(Scopes.SINGLETON);
            }
            catch (NoSuchMethodException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
