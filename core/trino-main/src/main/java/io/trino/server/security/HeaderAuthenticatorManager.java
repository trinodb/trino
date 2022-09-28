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
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.security.HeaderAuthenticator;
import io.trino.spi.security.HeaderAuthenticatorFactory;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.airlift.configuration.ConfigurationLoader.loadPropertiesFrom;

public class HeaderAuthenticatorManager
{
    private static final Logger log = Logger.get(HeaderAuthenticatorManager.class);
    private static final String NAME_PROPERTY = "header-authenticator.name";

    private final List<File> configFiles;
    private final AtomicBoolean required = new AtomicBoolean();
    private final Map<String, HeaderAuthenticatorFactory> factories = new ConcurrentHashMap<>();
    private final AtomicReference<List<HeaderAuthenticator>> authenticators = new AtomicReference<>();

    @Inject
    public HeaderAuthenticatorManager(HeaderAuthenticatorConfig config)
    {
        this.configFiles = ImmutableList.copyOf(config.getHeaderAuthenticatorFiles());
        checkArgument(!configFiles.isEmpty(), "header authenticator files list is empty");
    }

    public List<HeaderAuthenticator> getAuthenticators()
    {
        checkState(isLoaded(), "authenticators were not loaded");
        return this.authenticators.get();
    }

    public void addHeaderAuthenticatorFactory(HeaderAuthenticatorFactory factory)
    {
        checkArgument(this.factories.putIfAbsent(factory.getName(), factory) == null,
                "Header authenticator '%s' is already registered", factory.getName());
    }

    public void loadHeaderAuthenticator()
    {
        if (!required.get()) {
            return;
        }

        ImmutableList.Builder<HeaderAuthenticator> authenticators = ImmutableList.builder();
        for (File configFile : configFiles) {
            authenticators.add(loadAuthenticator(configFile.getAbsoluteFile()));
        }

        this.authenticators.set(authenticators.build());
    }

    private HeaderAuthenticator loadAuthenticator(File configFile)
    {
        Map<String, String> properties;
        try {
            properties = new HashMap<>(loadPropertiesFrom(configFile.getPath()));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        String name = properties.remove(NAME_PROPERTY);
        checkState(!isNullOrEmpty(name), "Header authenticator configuration %s does not contain '%s'", configFile, NAME_PROPERTY);

        log.info("-- Loading header authenticator --");

        HeaderAuthenticatorFactory factory = factories.get(name);
        checkState(factory != null, "Header authenticator '%s' is not registered", name);

        HeaderAuthenticator authenticator;
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(factory.getClass().getClassLoader())) {
            authenticator = factory.create(ImmutableMap.copyOf(properties));
        }

        log.info("-- Loaded header authenticator %s --", name);
        return authenticator;
    }

    public void setRequired()
    {
        required.set(true);
    }

    public boolean isLoaded()
    {
        return authenticators.get() != null;
    }
}
