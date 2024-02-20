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
import io.trino.spi.function.OperatorType;

import java.util.Locale;

import static com.google.common.base.Preconditions.checkArgument;

public final class OperatorNameUtil
{
    private static final String OPERATOR_PREFIX = "$operator$";

    private OperatorNameUtil() {}

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
        checkArgument(mangledName.startsWith(OPERATOR_PREFIX), "not a mangled operator name: %s", mangledName);
        return OperatorType.valueOf(mangledName.substring(OPERATOR_PREFIX.length()).toUpperCase(Locale.ENGLISH));
    }
}
