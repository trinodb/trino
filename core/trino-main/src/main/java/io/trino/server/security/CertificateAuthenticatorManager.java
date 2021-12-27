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

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.security.CertificateAuthenticator;
import io.trino.spi.security.CertificateAuthenticatorFactory;

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

public class CertificateAuthenticatorManager
{
    private static final Logger log = Logger.get(CertificateAuthenticatorManager.class);

    private static final File CONFIG_FILE = new File("etc/certificate-authenticator.properties");
    private static final String NAME_PROPERTY = "certificate-authenticator.name";

    private final AtomicBoolean required = new AtomicBoolean();
    private final Map<String, CertificateAuthenticatorFactory> factories = new ConcurrentHashMap<>();
    private final AtomicReference<CertificateAuthenticator> authenticator = new AtomicReference<>();

    public void setRequired()
    {
        required.set(true);
    }

    public void addCertificateAuthenticatorFactory(CertificateAuthenticatorFactory factory)
    {
        checkArgument(factories.putIfAbsent(factory.getName(), factory) == null,
                "Certificate authenticator '%s' is already registered", factory.getName());
    }

    public void loadCertificateAuthenticator()
            throws Exception
    {
        if (!required.get()) {
            return;
        }

        File configFile = CONFIG_FILE.getAbsoluteFile();
        if (!configFile.exists()) {
            useDefaultAuthenticator();
            return;
        }

        Map<String, String> properties = new HashMap<>(loadPropertiesFrom(configFile.getPath()));

        String name = properties.remove(NAME_PROPERTY);
        checkState(!isNullOrEmpty(name), "Certificate authenticator configuration %s does not contain '%s'", configFile, NAME_PROPERTY);

        log.info("-- Loading certificate authenticator --");

        CertificateAuthenticatorFactory factory = factories.get(name);
        checkState(factory != null, "Certificate authenticator '%s' is not registered", name);

        CertificateAuthenticator authenticator;
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(factory.getClass().getClassLoader())) {
            authenticator = factory.create(ImmutableMap.copyOf(properties));
        }

        this.authenticator.set(requireNonNull(authenticator, "authenticator is null"));

        log.info("-- Loaded certificate authenticator %s --", name);
    }

    public CertificateAuthenticator getAuthenticator()
    {
        checkState(authenticator.get() != null, "authenticator was not loaded");
        return authenticator.get();
    }

    public void useDefaultAuthenticator()
    {
        authenticator.set(certificates -> certificates.get(0).getSubjectX500Principal());
    }
}
