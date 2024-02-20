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
package io.trino.hdfs.cos;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

public class ServiceConfig
{
    private static final String ACCESS_KEY_SUFFIX = ".access-key";
    private static final String SECRET_KEY_SUFFIX = ".secret-key";
    private static final String ENDPOINT_SUFFIX = ".endpoint";
    private final String name;
    private final String accessKey;
    private final String secretKey;
    private final Optional<String> endpoint;

    public ServiceConfig(String name, String accessKey, String secretKey, Optional<String> endpoint)
    {
        this.name = requireNonNull(name, "name is null");
        this.accessKey = requireNonNull(accessKey, "accessKey is null");
        this.secretKey = requireNonNull(secretKey, "secretKey is null");
        this.endpoint = requireNonNull(endpoint, "endpoint is null");
    }

    public String getName()
    {
        return name;
    }

    public String getAccessKey()
    {
        return accessKey;
    }

    public String getSecretKey()
    {
        return secretKey;
    }

    public Optional<String> getEndpoint()
    {
        return endpoint;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .toString();
    }

    public static Map<String, ServiceConfig> loadServiceConfigs(File configFile)
    {
        if (configFile == null) {
            return ImmutableMap.of();
        }
        Properties properties = new Properties();
        try {
            properties.load(new FileInputStream(configFile));
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        Set<String> serviceNames = properties.keySet().stream()
                .map(String.class::cast)
                .map(ServiceConfig::getServiceName)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toImmutableSet());

        ImmutableMap.Builder<String, ServiceConfig> configs = ImmutableMap.builder();
        Set<String> usedProperties = new HashSet<>();
        for (String serviceName : serviceNames) {
            String accessKey = getRequiredProperty(serviceName + ACCESS_KEY_SUFFIX, properties, configFile, usedProperties);
            String secretKey = getRequiredProperty(serviceName + SECRET_KEY_SUFFIX, properties, configFile, usedProperties);
            Optional<String> endpoint = getOptionalProperty(serviceName + ENDPOINT_SUFFIX, properties, usedProperties);
            configs.put(serviceName, new ServiceConfig(serviceName, accessKey, secretKey, endpoint));
        }

        Set<Object> unusedProperties = Sets.difference(properties.keySet(), usedProperties);
        checkArgument(unusedProperties.isEmpty(), "Not all properties in file %s were used: %s", configFile, unusedProperties);

        return configs.buildOrThrow();
    }

    private static Optional<String> getServiceName(String propertyName)
    {
        if (propertyName.endsWith(ACCESS_KEY_SUFFIX)) {
            return Optional.of(propertyName.substring(0, propertyName.length() - ACCESS_KEY_SUFFIX.length()));
        }
        if (propertyName.endsWith(SECRET_KEY_SUFFIX)) {
            return Optional.of(propertyName.substring(0, propertyName.length() - SECRET_KEY_SUFFIX.length()));
        }
        if (propertyName.endsWith(ENDPOINT_SUFFIX)) {
            return Optional.of(propertyName.substring(0, propertyName.length() - ENDPOINT_SUFFIX.length()));
        }
        return Optional.empty();
    }

    private static String getRequiredProperty(String propertyName, Properties properties, File configFile, Set<String> usedProperties)
    {
        String value = properties.getProperty(propertyName);
        checkArgument(value != null, "%s bucket property not provided in file %s", propertyName, configFile);
        usedProperties.add(propertyName);
        return value;
    }

    private static Optional<String> getOptionalProperty(String propertyName, Properties properties, Set<String> usedProperties)
    {
        String value = properties.getProperty(propertyName);
        if (value != null) {
            usedProperties.add(propertyName);
        }
        return Optional.ofNullable(value);
    }
}
