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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Maps;
import com.google.common.primitives.Primitives;
import io.trino.Session;
import io.trino.security.AccessControl;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.Location;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.ParameterRewriter;
import io.trino.sql.tree.Array;
import io.trino.sql.tree.BooleanLiteral;
import io.trino.sql.tree.DoubleLiteral;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.ExpressionTreeRewriter;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.Parameter;
import io.trino.sql.tree.Property;
import io.trino.sql.tree.StringLiteral;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.sql.analyzer.ConstantEvaluator.evaluateConstant;
import static io.trino.sql.analyzer.ExpressionTreeUtils.extractLocation;
import static io.trino.util.MoreLists.mappedCopy;
import static java.util.Locale.ENGLISH;

public final class PropertyUtil
{
    private PropertyUtil() {}

    public static Map<String, Optional<Object>> evaluateProperties(
            Iterable<Property> setProperties,
            Session session,
            PlannerContext plannerContext,
            AccessControl accessControl,
            Map<NodeRef<Parameter>, Expression> parameters,
            boolean includeAllProperties,
            Map<String, PropertyMetadata<?>> metadata,
            ErrorCodeSupplier errorCode,
            String propertyTypeDescription)
    {
        Map<String, Optional<Object>> propertyValues = new LinkedHashMap<>();

        // Fill in user-specified properties
        for (Property property : setProperties) {
            Optional<Location> location = extractLocation(property);
            // property names are case-insensitive and normalized to lower case
            String propertyName = property.getName().getValue().toLowerCase(ENGLISH);
            PropertyMetadata<?> propertyMetadata = metadata.get(propertyName);
            if (propertyMetadata == null) {
                String message = "%s '%s' does not exist".formatted(capitalize(propertyTypeDescription), propertyName);
                throw new TrinoException(errorCode, location, message, null);
            }

            Optional<Object> value;
            if (property.isSetToDefault()) {
                value = Optional.ofNullable(propertyMetadata.getDefaultValue());
            }
            else {
                value = Optional.of(evaluateProperty(
                        location,
                        property.getNonDefaultValue(),
                        propertyMetadata,
                        session,
                        plannerContext,
                        accessControl,
                        parameters,
                        errorCode,
                        propertyTypeDescription));
            }

            propertyValues.put(propertyMetadata.getName(), value);
        }

        if (includeAllProperties) {
            for (PropertyMetadata<?> propertyMetadata : metadata.values()) {
                if (!propertyValues.containsKey(propertyMetadata.getName())) {
                    propertyValues.put(propertyMetadata.getName(), Optional.ofNullable(propertyMetadata.getDefaultValue()));
                }
            }
        }
        return ImmutableMap.copyOf(propertyValues);
    }

    private static Object evaluateProperty(
            Optional<Location> location,
            Expression expression,
            PropertyMetadata<?> property,
            Session session,
            PlannerContext plannerContext,
            AccessControl accessControl,
            Map<NodeRef<Parameter>, Expression> parameters,
            ErrorCodeSupplier errorCode,
            String propertyTypeDescription)
    {
        Object sqlObjectValue = evaluateProperty(
                location,
                property.getName(),
                property.getSqlType(),
                expression,
                session,
                plannerContext,
                accessControl,
                parameters,
                errorCode,
                propertyTypeDescription);

        try {
            return property.decode(sqlObjectValue);
        }
        catch (Exception e) {
            String message = "Unable to set %s '%s' to [%s]: %s".formatted(propertyTypeDescription, property.getName(), expression, e.getMessage());
            throw new TrinoException(errorCode, location, message, e);
        }
    }

    public static Object evaluateProperty(
            Optional<Location> location,
            String propertyName,
            Type propertyType,
            Expression expression,
            Session session,
            PlannerContext plannerContext,
            AccessControl accessControl,
            Map<NodeRef<Parameter>, Expression> parameters,
            ErrorCodeSupplier errorCode,
            String propertyTypeDescription)
    {
        Object sqlObjectValue;
        try {
            Expression rewritten = ExpressionTreeRewriter.rewriteWith(new ParameterRewriter(parameters), expression);
            Object value = evaluateConstant(rewritten, propertyType, plannerContext, session, accessControl);

            // convert to object value type of SQL type
            Block block = writeNativeValue(propertyType, value);
            sqlObjectValue = propertyType.getObjectValue(block, 0);
        }
        catch (TrinoException e) {
            String message = "Invalid value for %s '%s': Cannot convert [%s] to %s".formatted(propertyTypeDescription, propertyName, expression, propertyType);
            throw new TrinoException(errorCode, location, message, e);
        }

        if (sqlObjectValue == null) {
            String message = "Invalid null value for %s '%s' from [%s]".formatted(propertyTypeDescription, propertyName, expression);
            throw new TrinoException(errorCode, location, message, null);
        }
        return sqlObjectValue;
    }

    public static List<Property> toSqlProperties(
            Object description,
            ErrorCodeSupplier errorCode,
            Map<String, Object> properties,
            Iterable<PropertyMetadata<?>> metadata)
    {
        if (properties.isEmpty()) {
            return List.of();
        }

        Map<String, PropertyMetadata<?>> indexedMetadata = Maps.uniqueIndex(metadata, PropertyMetadata::getName);
        ImmutableSortedMap.Builder<String, Expression> sqlProperties = ImmutableSortedMap.naturalOrder();

        properties.forEach((name, value) -> {
            if (value == null) {
                throw new TrinoException(errorCode, "Property %s for %s cannot have a null value".formatted(name, description));
            }

            PropertyMetadata<?> property = indexedMetadata.get(name);
            if (property == null) {
                throw new TrinoException(errorCode, "No PropertyMetadata for property: " + name);
            }
            if (!Primitives.wrap(property.getJavaType()).isInstance(value)) {
                throw new TrinoException(errorCode, "Property %s for %s should have value of type %s, not %s"
                        .formatted(name, description, property.getJavaType().getName(), value.getClass().getName()));
            }

            sqlProperties.put(name, toExpression(errorCode, property, value));
        });

        return sqlProperties.build().entrySet().stream()
                .map(entry -> new Property(new Identifier(entry.getKey()), entry.getValue()))
                .collect(toImmutableList());
    }

    private static <T> Expression toExpression(ErrorCodeSupplier errorcode, PropertyMetadata<T> property, Object value)
            throws TrinoException
    {
        return toExpression(errorcode, property.encode(property.getJavaType().cast(value)));
    }

    private static Expression toExpression(ErrorCodeSupplier errorCode, Object value)
            throws TrinoException
    {
        return switch (value) {
            case String _ -> new StringLiteral(value.toString());
            case Boolean _ -> new BooleanLiteral(value.toString());
            case Long _, Integer _ -> new LongLiteral(value.toString());
            case Double _ -> new DoubleLiteral(value.toString());
            case List<?> list -> new Array(mappedCopy(list, item -> toExpression(errorCode, item)));
            case null -> throw new TrinoException(errorCode, "Property value is null");
            default -> throw new TrinoException(errorCode, "Failed to convert object of type %s to expression".formatted(value.getClass().getName()));
        };
    }

    private static String capitalize(String value)
    {
        return Character.toUpperCase(value.charAt(0)) + value.substring(1);
    }
}
