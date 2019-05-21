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
package io.prestosql.plugin.password.file;

import com.google.inject.Injector;
import com.google.inject.Scopes;
import io.airlift.bootstrap.Bootstrap;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.security.PasswordAuthenticator;
import io.prestosql.spi.security.PasswordAuthenticatorFactory;

import java.util.Map;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.prestosql.spi.StandardErrorCode.CONFIGURATION_INVALID;
import static java.lang.String.format;

public class FileBasedAuthenticatorFactory
        implements PasswordAuthenticatorFactory
{
    @Override
    public String getName()
    {
        return "file";
    }

    @Override
    public PasswordAuthenticator create(Map<String, String> config)
    {
        Bootstrap app = new Bootstrap(
                binder -> {
                    configBinder(binder).bindConfig(FileBasedAuthenticationConfig.class);
                    binder.bind(FileBasedAuthenticator.class).in(Scopes.SINGLETON);
                });

        Injector injector = app
                .strictConfig()
                .doNotInitializeLogging()
                .setRequiredConfigurationProperties(config)
                .initialize();

        return injector.getInstance(FileBasedAuthenticator.class);
    }

    private PrestoException invalidRefreshPeriodException(Map<String, String> config)
    {
        return new PrestoException(
                CONFIGURATION_INVALID,
            format("Invalid duration value '%s' for property '%s' in file", config.get("file.refresh-period"), "file.refresh-period"));
    }
}
