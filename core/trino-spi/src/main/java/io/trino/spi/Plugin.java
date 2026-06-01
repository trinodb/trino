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
package io.trino.spi;

import io.trino.spi.block.BlockEncoding;
import io.trino.spi.catalog.CatalogStoreFactory;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.eventlistener.EventListenerFactory;
import io.trino.spi.exchange.ExchangeManagerFactory;
import io.trino.spi.function.LanguageFunctionEngine;
import io.trino.spi.resourcegroups.ResourceGroupConfigurationManagerFactory;
import io.trino.spi.security.CertificateAuthenticatorFactory;
import io.trino.spi.security.GroupProviderFactory;
import io.trino.spi.security.HeaderAuthenticatorFactory;
import io.trino.spi.security.PasswordAuthenticatorFactory;
import io.trino.spi.security.SystemAccessControlFactory;
import io.trino.spi.session.SessionPropertyConfigurationManagerFactory;
import io.trino.spi.spool.SpoolingManagerFactory;
import io.trino.spi.type.ParametricType;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Set;

public interface Plugin
{
    default Iterable<CatalogStoreFactory> getCatalogStoreFactories()
    {
        return List.of();
    }

    default Iterable<ConnectorFactory> getConnectorFactories()
    {
        return List.of();
    }

    default Iterable<BlockEncoding> getBlockEncodings()
    {
        return List.of();
    }

    default Iterable<Type> getTypes()
    {
        return List.of();
    }

    default Iterable<ParametricType> getParametricTypes()
    {
        return List.of();
    }

    default Set<Class<?>> getFunctions()
    {
        return Set.of();
    }

    default Iterable<LanguageFunctionEngine> getLanguageFunctionEngines()
    {
        return List.of();
    }

    default Iterable<SystemAccessControlFactory> getSystemAccessControlFactories()
    {
        return List.of();
    }

    default Iterable<GroupProviderFactory> getGroupProviderFactories()
    {
        return List.of();
    }

    default Iterable<PasswordAuthenticatorFactory> getPasswordAuthenticatorFactories()
    {
        return List.of();
    }

    default Iterable<HeaderAuthenticatorFactory> getHeaderAuthenticatorFactories()
    {
        return List.of();
    }

    default Iterable<CertificateAuthenticatorFactory> getCertificateAuthenticatorFactories()
    {
        return List.of();
    }

    default Iterable<EventListenerFactory> getEventListenerFactories()
    {
        return List.of();
    }

    default Iterable<ResourceGroupConfigurationManagerFactory> getResourceGroupConfigurationManagerFactories()
    {
        return List.of();
    }

    default Iterable<SessionPropertyConfigurationManagerFactory> getSessionPropertyConfigurationManagerFactories()
    {
        return List.of();
    }

    default Iterable<ExchangeManagerFactory> getExchangeManagerFactories()
    {
        return List.of();
    }

    default Iterable<SpoolingManagerFactory> getSpoolingManagerFactories()
    {
        return List.of();
    }
}
