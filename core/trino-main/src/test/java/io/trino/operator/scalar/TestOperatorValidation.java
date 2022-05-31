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
package io.trino.operator.scalar;

import io.trino.metadata.InternalFunctionBundle;
import io.trino.spi.function.ScalarOperator;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;
import org.testng.annotations.Test;

import static io.trino.spi.function.OperatorType.ADD;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@SuppressWarnings("UtilityClassWithoutPrivateConstructor")
public class TestOperatorValidation
{
    @Test
    public void testInvalidArgumentCount()
    {
        assertThatThrownBy(() -> extractScalars(InvalidArgumentCount.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageMatching("ADD operator must have exactly 2 argument.*");
    }

    public static final class InvalidArgumentCount
    {
        @ScalarOperator(ADD)
        @SqlType(StandardTypes.BIGINT)
        public static long add(@SqlType(StandardTypes.BIGINT) long value)
        {
            return 0;
        }
    }

    private static void extractScalars(Class<?> clazz)
    {
        InternalFunctionBundle.builder().scalars(clazz);
    }
}
