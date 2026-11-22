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

import io.trino.spi.function.OperatorType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import static io.trino.metadata.OperatorNameUtil.isOperatorName;
import static io.trino.metadata.OperatorNameUtil.mangleOperatorName;
import static io.trino.metadata.OperatorNameUtil.unmangleOperator;
import static io.trino.spi.function.OperatorType.CAST;
import static io.trino.spi.function.OperatorType.IDENTICAL;
import static io.trino.spi.function.OperatorType.LESS_THAN;
import static java.util.Locale.ENGLISH;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestOperatorNameUtil
{
    @ParameterizedTest
    @EnumSource(OperatorType.class)
    void testOperatorNames(OperatorType operatorType)
    {
        String name = mangleOperatorName(operatorType);
        assertThat(name).isEqualTo("$operator$" + operatorType.name());
        assertThat(isOperatorName(name)).isTrue();
        assertThat(unmangleOperator(name)).isEqualTo(operatorType);
        assertThat(unmangleOperator(name.toLowerCase(ENGLISH))).isEqualTo(operatorType);
        assertThat(unmangleOperator("$operator$" + operatorType.name().charAt(0) + operatorType.name().substring(1).toLowerCase(ENGLISH)))
                .isEqualTo(operatorType);
    }

    @Test
    void testUnicodeCaseConversion()
    {
        assertThat(unmangleOperator("$operator$le\u00df_than")).isEqualTo(LESS_THAN);
        assertThat(unmangleOperator("$operator$\u0131dent\u0131cal")).isEqualTo(IDENTICAL);
        assertThat(unmangleOperator("$operator$ca\u017ft")).isEqualTo(CAST);
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "add", "$operator", "$Operator$add", "$OPERATOR$ADD"})
    void testInvalidPrefix(String name)
    {
        assertThat(isOperatorName(name)).isFalse();
        assertThatThrownBy(() -> unmangleOperator(name))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("not a mangled operator name: %s", name);
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "unknown", "add ", "$add", "hash code"})
    void testUnknownOperator(String name)
    {
        assertThat(isOperatorName("$operator$" + name)).isTrue();
        assertThatThrownBy(() -> unmangleOperator("$operator$" + name))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("No enum constant io.trino.spi.function.OperatorType.%s", name.toUpperCase(ENGLISH));
    }
}
