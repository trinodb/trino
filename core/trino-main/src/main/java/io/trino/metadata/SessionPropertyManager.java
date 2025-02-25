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
package io.trino.metadata;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.trino.Session;
import io.trino.SystemSessionProperties;
import io.trino.SystemSessionPropertiesProvider;
import io.trino.connector.CatalogServiceProvider;
import io.trino.security.AccessControl;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.CatalogHandle;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.ParameterRewriter;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.ExpressionTreeRewriter;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.Parameter;
import jakarta.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.sql.analyzer.ConstantEvaluator.evaluateConstant;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class SessionPropertyManager
{
    private static final JsonCodecFactory JSON_CODEC_FACTORY = new JsonCodecFactory();
    private final ConcurrentMap<String, PropertyMetadata<?>> systemSessionProperties = new ConcurrentHashMap<>();
    private final CatalogServiceProvider<Map<String, PropertyMetadata<?>>> connectorSessionProperties;

    public SessionPropertyManager()
    {
        this(new SystemSessionProperties());
    }

    public SessionPropertyManager(SystemSessionPropertiesProvider systemSessionPropertiesProvider)
    {
        this(ImmutableSet.of(systemSessionPropertiesProvider), connectorName -> ImmutableMap.of());
    }

    public SessionPropertyManager(Set<SystemSessionPropertiesProvider> systemSessionProperties, CatalogServiceProvider<Map<String, PropertyMetadata<?>>> connectorSessionProperties)
    {
        addSystemSessionProperties(systemSessionProperties
                .stream()
                .flatMap(provider -> provider.getSessionProperties().stream())
                .collect(toImmutableList()));
        this.connectorSessionProperties = requireNonNull(connectorSessionProperties, "connectorSessionProperties is null");
    }

    public void addSystemSessionProperties(List<PropertyMetadata<?>> systemSessionProperties)
    {
        systemSessionProperties
                .forEach(this::addSystemSessionProperty);
    }

    public void addSystemSessionProperty(PropertyMetadata<?> sessionProperty)
    {
        requireNonNull(sessionProperty, "sessionProperty is null");
        checkState(systemSessionProperties.put(sessionProperty.getName(), sessionProperty) == null,
                "System session property '%s' are already registered", sessionProperty.getName());
    }

    public Optional<PropertyMetadata<?>> getSystemSessionPropertyMetadata(String name)
    {
        requireNonNull(name, "name is null");

        return Optional.ofNullable(systemSessionProperties.get(name));
    }

    public Set<PropertyMetadata<?>> getSystemSessionPropertiesMetadata()
    {
        return ImmutableSet.copyOf(systemSessionProperties.values());
    }

    public Optional<PropertyMetadata<?>> getConnectorSessionPropertyMetadata(CatalogHandle catalogHandle, String propertyName)
    {
        requireNonNull(catalogHandle, "catalogHandle is null");
        requireNonNull(propertyName, "propertyName is null");
        Map<String, PropertyMetadata<?>> properties = connectorSessionProperties.getService(catalogHandle);
        return Optional.ofNullable(properties.get(propertyName));
    }

    public Set<PropertyMetadata<?>> getConnectionSessionPropertiesMetadata(CatalogHandle catalogHandle)
    {
        requireNonNull(catalogHandle, "catalogHandle is null");
        return ImmutableSet.copyOf(connectorSessionProperties.getService(catalogHandle).values());
    }

    public List<SessionPropertyValue> getAllSessionProperties(Session session, List<CatalogInfo> catalogInfos)
    {
        requireNonNull(session, "session is null");

        ImmutableList.Builder<SessionPropertyValue> sessionPropertyValues = ImmutableList.builder();
        Map<String, String> systemProperties = session.getSystemProperties();
        for (PropertyMetadata<?> property : new TreeMap<>(systemSessionProperties).values()) {
            String defaultValue = firstNonNull(property.getDefaultValue(), "").toString();
            String value = systemProperties.getOrDefault(property.getName(), defaultValue);
            sessionPropertyValues.add(new SessionPropertyValue(
                    value,
                    defaultValue,
                    property.getName(),
                    Optional.empty(),
                    property.getName(),
                    property.getDescription(),
                    property.getSqlType().getDisplayName(),
                    property.isHidden()));
        }

        for (CatalogInfo catalogInfo : catalogInfos) {
            CatalogHandle catalogHandle = catalogInfo.catalogHandle();
            String catalogName = catalogInfo.catalogName();
            Map<String, String> connectorProperties = session.getCatalogProperties(catalogName);

            for (PropertyMetadata<?> property : new TreeMap<>(connectorSessionProperties.getService(catalogHandle)).values()) {
                String defaultValue = firstNonNull(property.getDefaultValue(), "").toString();
                String value = connectorProperties.getOrDefault(property.getName(), defaultValue);

                sessionPropertyValues.add(new SessionPropertyValue(
                        value,
                        defaultValue,
                        catalogName + "." + property.getName(),
                        Optional.of(catalogName),
                        property.getName(),
                        property.getDescription(),
                        property.getSqlType().getDisplayName(),
                        property.isHidden()));
            }
        }

        return sessionPropertyValues.build();
    }

    public <T> T decodeSystemPropertyValue(String name, @Nullable String value, Class<T> type)
    {
        PropertyMetadata<?> property = getSystemSessionPropertyMetadata(name)
                .orElseThrow(() -> new TrinoException(INVALID_SESSION_PROPERTY, "Session property '%s' does not exist".formatted(name)));

        return decodePropertyValue(name, value, type, property);
    }

    public <T> T decodeCatalogPropertyValue(CatalogHandle catalogHandle, String catalogName, String propertyName, @Nullable String propertyValue, Class<T> type)
    {
        String fullPropertyName = catalogName + "." + propertyName;
        PropertyMetadata<?> property = getConnectorSessionPropertyMetadata(catalogHandle, propertyName)
                .orElseThrow(() -> new TrinoException(INVALID_SESSION_PROPERTY, "Session property '%s' does not exist".formatted(fullPropertyName)));

        return decodePropertyValue(fullPropertyName, propertyValue, type, property);
    }

    public void validateSystemSessionProperty(String propertyName, String propertyValue)
    {
        PropertyMetadata<?> propertyMetadata = getSystemSessionPropertyMetadata(propertyName)
                .orElseThrow(() -> new TrinoException(INVALID_SESSION_PROPERTY, "Session property '%s' does not exist".formatted(propertyName)));

        decodePropertyValue(propertyName, propertyValue, propertyMetadata.getJavaType(), propertyMetadata);
    }

    public void validateCatalogSessionProperty(String catalogName, CatalogHandle catalogHandle, String propertyName, String propertyValue)
    {
        String fullPropertyName = catalogName + "." + propertyName;
        PropertyMetadata<?> propertyMetadata = getConnectorSessionPropertyMetadata(catalogHandle, propertyName)
                .orElseThrow(() -> new TrinoException(INVALID_SESSION_PROPERTY, "Session property '%s' does not exist".formatted(fullPropertyName)));

        decodePropertyValue(fullPropertyName, propertyValue, propertyMetadata.getJavaType(), propertyMetadata);
    }

    private static <T> T decodePropertyValue(String fullPropertyName, @Nullable String propertyValue, Class<T> type, PropertyMetadata<?> metadata)
    {
        if (metadata.getJavaType() != type) {
            throw new TrinoException(INVALID_SESSION_PROPERTY, format("Session property '%s' has type '%s', but requested type was %s", fullPropertyName,
                    metadata.getJavaType().getName(),
                    type.getName()));
        }
        if (propertyValue == null) {
            return type.cast(metadata.getDefaultValue());
        }
        Object objectValue = deserializeSessionProperty(metadata.getSqlType(), propertyValue);
        try {
            return type.cast(metadata.decode(objectValue));
        }
        catch (TrinoException e) {
            throw e;
        }
        catch (Exception e) {
            // the system property decoder can throw any exception
            throw new TrinoException(INVALID_SESSION_PROPERTY, format("Session property '%s' is invalid: %s".formatted(fullPropertyName, e.getMessage())));
        }
    }

    public static Object evaluatePropertyValue(Expression expression, Type expectedType, Session session, PlannerContext plannerContext, AccessControl accessControl, Map<NodeRef<Parameter>, Expression> parameters)
    {
        Expression rewritten = ExpressionTreeRewriter.rewriteWith(new ParameterRewriter(parameters), expression);
        Object value = evaluateConstant(rewritten, expectedType, plannerContext, session, accessControl);

        // convert to object value type of SQL type
        Block block = writeNativeValue(expectedType, value);
        Object objectValue = expectedType.getObjectValue(session.toConnectorSession(), block, 0);

        if (objectValue == null) {
            throw new TrinoException(INVALID_SESSION_PROPERTY, "Session property value must not be null");
        }
        return objectValue;
    }

    public static String serializeSessionProperty(Type type, Object value)
    {
        if (value == null) {
            throw new TrinoException(INVALID_SESSION_PROPERTY, "Session property cannot be null");
        }
        if (BooleanType.BOOLEAN.equals(type)) {
            return value.toString();
        }
        if (BigintType.BIGINT.equals(type)) {
            return value.toString();
        }
        if (IntegerType.INTEGER.equals(type)) {
            return value.toString();
        }
        if (DoubleType.DOUBLE.equals(type)) {
            return value.toString();
        }
        if (VarcharType.VARCHAR.equals(type)) {
            return value.toString();
        }
        if (type instanceof ArrayType || type instanceof MapType) {
            return getJsonCodecForType(type).toJson(value);
        }
        throw new TrinoException(INVALID_SESSION_PROPERTY, format("Session property type '%s' is not supported", type));
    }

    private static Object deserializeSessionProperty(Type type, String value)
    {
        if (value == null) {
            throw new TrinoException(INVALID_SESSION_PROPERTY, "Session property cannot be null");
        }
        if (VarcharType.VARCHAR.equals(type)) {
            return value;
        }
        if (BooleanType.BOOLEAN.equals(type)) {
            return Boolean.valueOf(value);
        }
        if (BigintType.BIGINT.equals(type)) {
            return Long.valueOf(value);
        }
        if (IntegerType.INTEGER.equals(type)) {
            return Integer.valueOf(value);
        }
        if (DoubleType.DOUBLE.equals(type)) {
            return Double.valueOf(value);
        }
        if (type instanceof ArrayType || type instanceof MapType) {
            return getJsonCodecForType(type).fromJson(value);
        }
        throw new TrinoException(INVALID_SESSION_PROPERTY, format("Session property type '%s' is not supported", type));
    }

    private static <T> JsonCodec<T> getJsonCodecForType(Type type)
    {
        if (VarcharType.VARCHAR.equals(type)) {
            return (JsonCodec<T>) JSON_CODEC_FACTORY.jsonCodec(String.class);
        }
        if (BooleanType.BOOLEAN.equals(type)) {
            return (JsonCodec<T>) JSON_CODEC_FACTORY.jsonCodec(Boolean.class);
        }
        if (BigintType.BIGINT.equals(type)) {
            return (JsonCodec<T>) JSON_CODEC_FACTORY.jsonCodec(Long.class);
        }
        if (IntegerType.INTEGER.equals(type)) {
            return (JsonCodec<T>) JSON_CODEC_FACTORY.jsonCodec(Integer.class);
        }
        if (DoubleType.DOUBLE.equals(type)) {
            return (JsonCodec<T>) JSON_CODEC_FACTORY.jsonCodec(Double.class);
        }
        if (type instanceof ArrayType arrayType) {
            Type elementType = arrayType.getElementType();
            return (JsonCodec<T>) JSON_CODEC_FACTORY.listJsonCodec(getJsonCodecForType(elementType));
        }
        if (type instanceof MapType mapType) {
            Type keyType = mapType.getKeyType();
            Type valueType = mapType.getValueType();
            return (JsonCodec<T>) JSON_CODEC_FACTORY.mapJsonCodec(getMapKeyType(keyType), getJsonCodecForType(valueType));
        }
        throw new TrinoException(INVALID_SESSION_PROPERTY, format("Session property type '%s' is not supported", type));
    }

    private static Class<?> getMapKeyType(Type type)
    {
        if (VarcharType.VARCHAR.equals(type)) {
            return String.class;
        }
        if (BooleanType.BOOLEAN.equals(type)) {
            return Boolean.class;
        }
        if (BigintType.BIGINT.equals(type)) {
            return Long.class;
        }
        if (IntegerType.INTEGER.equals(type)) {
            return Integer.class;
        }
        if (DoubleType.DOUBLE.equals(type)) {
            return Double.class;
        }
        throw new TrinoException(INVALID_SESSION_PROPERTY, format("Session property map key type '%s' is not supported", type));
    }

    public static class SessionPropertyValue
    {
        private final String fullyQualifiedName;
        private final Optional<String> catalogName;
        private final String propertyName;
        private final String description;
        private final String type;
        private final String value;
        private final String defaultValue;
        private final boolean hidden;

        private SessionPropertyValue(
                String value,
                String defaultValue,
                String fullyQualifiedName,
                Optional<String> catalogName,
                String propertyName,
                String description,
                String type,
                boolean hidden)
        {
            this.fullyQualifiedName = fullyQualifiedName;
            this.catalogName = catalogName;
            this.propertyName = propertyName;
            this.description = description;
            this.type = type;
            this.value = value;
            this.defaultValue = defaultValue;
            this.hidden = hidden;
        }

        public String getFullyQualifiedName()
        {
            return fullyQualifiedName;
        }

        public Optional<String> getCatalogName()
        {
            return catalogName;
        }

        public String getPropertyName()
        {
            return propertyName;
        }

        public String getDescription()
        {
            return description;
        }

        public String getType()
        {
            return type;
        }

        public String getValue()
        {
            return value;
        }

        public String getDefaultValue()
        {
            return defaultValue;
        }

        public boolean isHidden()
        {
            return hidden;
        }
    }
}
