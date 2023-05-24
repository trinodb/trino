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
package io.trino.server;

import com.google.common.collect.ImmutableList;
import io.trino.spi.Plugin;
import io.trino.spi.classloader.ThreadContextClassLoader;

import java.io.File;
import java.util.List;
import java.util.ServiceLoader;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.trino.metadata.InternalFunctionBundle.extractFunctions;
import static java.util.Arrays.asList;

public class PluginLoader
{
    public static final String CONNECTOR = "connector:";
    public static final String BLOCK_ENCODING = "blockEncoding:";
    public static final String PARAMETRIC_TYPE = "parametricType:";
    public static final String FUNCTION = "function:";
    public static final String SYSTEM_ACCESS_CONTROL = "systemAccessControl:";
    public static final String GROUP_PROVIDER = "groupProvider:";
    public static final String PASSWORD_AUTHENTICATOR = "passwordAuthenticator:";
    public static final String HEADER_AUTHENTICATOR = "headerAuthenticator:";
    public static final String CERTIFICATE_AUTHENTICATOR = "certificateAuthenticator:";
    public static final String EVENT_LISTENER = "eventListener:";
    public static final String RESOURCE_GROUP_CONFIGURATION_MANAGER = "resourceGroupConfigurationManager:";
    public static final String SESSION_PROPERTY_CONFIGURATION_MANAGER = "sessionPropertyConfigurationManager:";
    public static final String EXCHANGE_MANAGER = "exchangeManager:";

    private PluginLoader() {}

    public static void printPluginFeatures(Plugin plugin)
    {
        plugin.getConnectorFactories().forEach(factory -> System.out.println(CONNECTOR + factory.getName()));
        plugin.getBlockEncodings().forEach(encoding -> System.out.println(BLOCK_ENCODING + encoding.getName()));
        plugin.getTypes().forEach(type -> System.out.println(type.getTypeId()));
        plugin.getParametricTypes().forEach(type -> System.out.println(PARAMETRIC_TYPE + type.getName()));
        plugin.getFunctions().forEach(functionClass -> extractFunctions(functionClass)
                .getFunctions()
                .forEach(function -> System.out.println(FUNCTION + function.getSignature())));
        plugin.getSystemAccessControlFactories().forEach(factory -> System.out.println(SYSTEM_ACCESS_CONTROL + factory.getName()));
        plugin.getGroupProviderFactories().forEach(factory -> System.out.println(GROUP_PROVIDER + factory.getName()));
        plugin.getPasswordAuthenticatorFactories().forEach(factory -> System.out.println(PASSWORD_AUTHENTICATOR + factory.getName()));
        plugin.getHeaderAuthenticatorFactories().forEach(factory -> System.out.println(HEADER_AUTHENTICATOR + factory.getName()));
        plugin.getCertificateAuthenticatorFactories().forEach(factory -> System.out.println(CERTIFICATE_AUTHENTICATOR + factory.getName()));
        plugin.getEventListenerFactories().forEach(factory -> System.out.println(EVENT_LISTENER + factory.getName()));
        plugin.getResourceGroupConfigurationManagerFactories().forEach(factory -> System.out.println(RESOURCE_GROUP_CONFIGURATION_MANAGER + factory.getName()));
        plugin.getSessionPropertyConfigurationManagerFactories().forEach(factory -> System.out.println(SESSION_PROPERTY_CONFIGURATION_MANAGER + factory.getName()));
        plugin.getExchangeManagerFactories().forEach(factory -> System.out.println(EXCHANGE_MANAGER + factory.getName()));
    }

    public static List<Plugin> loadPlugins(File path)
    {
        ServerPluginsProviderConfig config = new ServerPluginsProviderConfig();
        config.setInstalledPluginsDir(path);
        ServerPluginsProvider pluginsProvider = new ServerPluginsProvider(config, directExecutor());
        ImmutableList.Builder<Plugin> plugins = ImmutableList.builder();
        pluginsProvider.loadPlugins((plugin, createClassLoader) -> loadPlugin(createClassLoader, plugins), PluginManager::createClassLoader);
        return plugins.build();
    }

    private static void loadPlugin(Supplier<PluginClassLoader> createClassLoader, ImmutableList.Builder<Plugin> plugins)
    {
        PluginClassLoader pluginClassLoader = createClassLoader.get();
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(pluginClassLoader)) {
            loadServicePlugin(pluginClassLoader, plugins);
        }
    }

    private static void loadServicePlugin(PluginClassLoader pluginClassLoader, ImmutableList.Builder<Plugin> plugins)
    {
        ServiceLoader<Plugin> serviceLoader = ServiceLoader.load(Plugin.class, pluginClassLoader);
        List<Plugin> loadedPlugins = ImmutableList.copyOf(serviceLoader);
        checkState(!loadedPlugins.isEmpty(), "No service providers of type %s in the classpath: %s", Plugin.class.getName(), asList(pluginClassLoader.getURLs()));
        plugins.addAll(loadedPlugins);
    }
}
