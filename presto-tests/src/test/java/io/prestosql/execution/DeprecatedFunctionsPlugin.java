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
package io.prestosql.execution;

import com.google.common.collect.ImmutableSet;
import io.prestosql.spi.Plugin;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.StandardTypes;

import java.util.Set;

public class DeprecatedFunctionsPlugin
        implements Plugin
{
    @Override
    public Set<Class<?>> getFunctions()
    {
        return ImmutableSet.<Class<?>>builder()
                .add(TestPluginScalaFunction.class)
                .build();
    }

    public static class TestPluginScalaFunction
    {
        @Deprecated
        @ScalarFunction("plugin_deprecated_scalar")
        @Description("Returns TRUE")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean deprecatedScalar()
        {
            return true;
        }

        @ScalarFunction("plugin_non_deprecated_scalar")
        @Description("Returns TRUE")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean nonDeprecatedScalar()
        {
            return true;
        }
    }
}
