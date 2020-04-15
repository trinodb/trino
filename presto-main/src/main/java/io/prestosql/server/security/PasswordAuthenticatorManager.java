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
package io.prestosql.server.security;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.prestosql.spi.security.PasswordAuthenticator;
import io.prestosql.spi.security.PasswordAuthenticatorFactory;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.airlift.configuration.ConfigurationLoader.loadPropertiesFrom;
import static java.util.Objects.requireNonNull;

public class PasswordAuthenticatorManager
{
    private static final Logger log = Logger.get(PasswordAuthenticatorManager.class);

    private static final File CONFIG_FILE = new File("etc/password-authenticator.properties");
    private static final String NAME_PROPERTY = "password-authenticator.name";

    private final AtomicBoolean required = new AtomicBoolean();
    private final Map<String, PasswordAuthenticatorFactory> factories = new ConcurrentHashMap<>();
    private final AtomicReference<PasswordAuthenticator> authenticator = new AtomicReference<>();

    public void setRequired()
    {
        required.set(true);
    }

    public void addPasswordAuthenticatorFactory(PasswordAuthenticatorFactory factory)
    {
        checkArgument(factories.putIfAbsent(factory.getName(), factory) == null,
                "Password authenticator '%s' is already registered", factory.getName());
    }

    public boolean isLoaded()
    {
        return authenticator.get() != null;
    }

    public void loadPasswordAuthenticator()
            throws Exception
    {
        if (!required.get()) {
            return;
        }

        File configFile = CONFIG_FILE.getAbsoluteFile();
        Map<String, String> properties = new HashMap<>(loadPropertiesFrom(configFile.getPath()));

        String name = properties.remove(NAME_PROPERTY);
        checkState(!isNullOrEmpty(name), "Password authenticator configuration %s does not contain '%s'", configFile, NAME_PROPERTY);

        log.info("-- Loading password authenticator --");

        PasswordAuthenticatorFactory factory = factories.get(name);
        checkState(factory != null, "Password authenticator '%s' is not registered", name);

        PasswordAuthenticator authenticator = factory.create(ImmutableMap.copyOf(properties));
        this.authenticator.set(requireNonNull(authenticator, "authenticator is null"));

        log.info("-- Loaded password authenticator %s --", name);
    }

    public PasswordAuthenticator getAuthenticator()
    {
        checkState(isLoaded(), "authenticator was not loaded");
        return authenticator.get();
    }

    @VisibleForTesting
    public void setAuthenticator(PasswordAuthenticator authenticator)
    {
        if (!this.authenticator.compareAndSet(null, authenticator)) {
            throw new IllegalStateException("authenticator already loaded");
        }
    }
}
