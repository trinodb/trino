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
import io.trino.Session;
import io.trino.security.AccessControl;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.session.PropertyMetadata;
import io.trino.spi.type.Type;
import io.trino.sql.PlannerContext;
import io.trino.sql.planner.ParameterRewriter;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.ExpressionTreeRewriter;
import io.trino.sql.tree.NodeRef;
import io.trino.sql.tree.Parameter;
import io.trino.sql.tree.Property;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static io.trino.sql.planner.ExpressionInterpreter.evaluateConstantExpression;
import static java.lang.String.format;

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
            String propertyName = property.getName().getCanonicalValue();
            PropertyMetadata<?> propertyMetadata = metadata.get(propertyName);
            if (propertyMetadata == null) {
                throw new TrinoException(errorCode, format("%s '%s' does not exist", capitalize(propertyTypeDescription), propertyName));
            }

            Optional<Object> value;
            if (property.isSetToDefault()) {
                value = Optional.ofNullable(propertyMetadata.getDefaultValue());
            }
            else {
                value = Optional.of(evaluateProperty(
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
            Expression expression,
            PropertyMetadata<?> property,
            Session session,
            PlannerContext plannerContext,
            AccessControl accessControl,
            Map<NodeRef<Parameter>, Expression> parameters,
            ErrorCodeSupplier errorCode,
            String propertyTypeDescription)
    {
        Object sqlObjectValue;
        try {
            Type expectedType = property.getSqlType();
            Expression rewritten = ExpressionTreeRewriter.rewriteWith(new ParameterRewriter(parameters), expression);
            Object value = evaluateConstantExpression(rewritten, expectedType, plannerContext, session, accessControl, parameters);

            // convert to object value type of SQL type
            BlockBuilder blockBuilder = expectedType.createBlockBuilder(null, 1);
            writeNativeValue(expectedType, blockBuilder, value);
            sqlObjectValue = expectedType.getObjectValue(session.toConnectorSession(), blockBuilder, 0);
        }
        catch (TrinoException e) {
            throw new TrinoException(
                    errorCode,
                    format(
                            "Invalid value for %s '%s': Cannot convert [%s] to %s",
                            propertyTypeDescription,
                            property.getName(),
                            expression,
                            property.getSqlType()),
                    e);
        }

        if (sqlObjectValue == null) {
            throw new TrinoException(
                    errorCode,
                    format(
                            "Invalid null value for %s '%s' from [%s]",
                            propertyTypeDescription,
                            property.getName(),
                            expression));
        }

        try {
            return property.decode(sqlObjectValue);
        }
        catch (Exception e) {
            throw new TrinoException(
                    errorCode,
                    format(
                            "Unable to set %s '%s' to [%s]: %s",
                            propertyTypeDescription,
                            property.getName(),
                            expression,
                            e.getMessage()),
                    e);
        }
    }

    private static String capitalize(String value)
    {
        return Character.toUpperCase(value.charAt(0)) + value.substring(1);
    }
}
