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
package io.trino.plugin.pinot.query;

import org.apache.pinot.common.function.FunctionRegistry;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.common.request.context.FunctionContext;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;
import static org.apache.pinot.common.function.TransformFunctionType.SCALAR;
import static org.apache.pinot.core.operator.transform.function.TransformFunctionFactory.canonicalize;

public final class PinotTransformFunctionTypeResolver
{
    private PinotTransformFunctionTypeResolver() {}

    private static final Map<String, TransformFunctionType> TRANSFORM_FUNCTION_TYPE_MAP;

    static
    {
        Map<String, TransformFunctionType> builder = new HashMap<>();
        for (TransformFunctionType transformFunctionType : TransformFunctionType.values()) {
            for (String alias : transformFunctionType.getAliases()) {
                TransformFunctionType previousValue = builder.put(canonicalize(alias), transformFunctionType);
                checkState(previousValue == null || previousValue == transformFunctionType, "Duplicate key with different values for alias '%s', transform function type '%s' and previous value '%s'", canonicalize(alias), transformFunctionType, previousValue);
            }
        }
        TRANSFORM_FUNCTION_TYPE_MAP = Map.copyOf(builder);
    }

    // Extracted from org.apache.pinot.core.operator.transform.function.TransformFunctionFactory::get
    public static Optional<TransformFunctionType> getTransformFunctionType(FunctionContext function)
    {
        requireNonNull(function, "function is null");
        String canonicalizedFunctionName = canonicalize(function.getFunctionName());
        TransformFunctionType transformFunctionType = TRANSFORM_FUNCTION_TYPE_MAP.get(canonicalizedFunctionName);
        if (transformFunctionType != null) {
            return Optional.of(transformFunctionType);
        }
        if (FunctionRegistry.containsFunction(canonicalizedFunctionName)) {
            return Optional.of(SCALAR);
        }
        return Optional.empty();
    }
}
