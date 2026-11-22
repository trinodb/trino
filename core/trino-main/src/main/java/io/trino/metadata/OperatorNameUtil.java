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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.function.OperatorType;

import java.util.Locale;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

public final class OperatorNameUtil
{
    private static final String OPERATOR_PREFIX = "$operator$";
    private static final Map<String, OperatorType> OPERATORS_BY_NAME = buildOperatorsByName();

    private OperatorNameUtil() {}

    private static Map<String, OperatorType> buildOperatorsByName()
    {
        ImmutableMap.Builder<String, OperatorType> operators = ImmutableMap.builder();
        for (OperatorType operatorType : OperatorType.values()) {
            String name = mangleOperatorName(operatorType);
            operators.put(name, operatorType);
            operators.put(name.toLowerCase(Locale.ENGLISH), operatorType);
        }
        return operators.buildOrThrow();
    }

    public static boolean isOperatorName(String mangledName)
    {
        return mangledName.startsWith(OPERATOR_PREFIX);
    }

    public static String mangleOperatorName(OperatorType operatorType)
    {
        return OPERATOR_PREFIX + operatorType.name();
    }

    @VisibleForTesting
    public static OperatorType unmangleOperator(String mangledName)
    {
        OperatorType operatorType = OPERATORS_BY_NAME.get(mangledName);
        if (operatorType != null) {
            return operatorType;
        }
        checkArgument(mangledName.startsWith(OPERATOR_PREFIX), "not a mangled operator name: %s", mangledName);
        return OperatorType.valueOf(mangledName.substring(OPERATOR_PREFIX.length()).toUpperCase(Locale.ENGLISH));
    }
}
