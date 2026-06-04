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

import io.trino.spi.function.Signature;
import io.trino.spi.function.Signature.Argument;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TypeTemplates.arrayType;
import static io.trino.spi.type.TypeTemplates.mapType;
import static io.trino.spi.type.TypeTemplates.type;
import static io.trino.spi.type.TypeTemplates.typeVariable;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestSignature
{
    @Test
    public void testArgumentNames()
    {
        Signature signature = Signature.builder()
                .returnType(VARCHAR.getTypeDescriptor())
                .argumentType(VARCHAR.getTypeDescriptor(), "string")
                .argumentType(BIGINT.getTypeDescriptor(), "from")
                .argumentType(BIGINT.getTypeDescriptor())
                .build();

        assertThat(signature.getArguments())
                .extracting(Argument::name)
                .containsExactly(Optional.of("string"), Optional.of("from"), Optional.empty());
    }

    @Test
    public void testTypeVariableReferencesMustBeStructural()
    {
        // A declared type variable smuggled as a parameterless application would bind by base name
        // but compare unequal to the canonical TypeVariable form and survive substitution unresolved
        assertThatThrownBy(() -> Signature.builder()
                .typeVariable("T")
                .returnType(type("T"))
                .argumentType(typeVariable("T"))
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be referenced as a type variable");

        // case-insensitive, like declaration matching
        assertThatThrownBy(() -> Signature.builder()
                .typeVariable("T")
                .returnType(typeVariable("T"))
                .argumentType(arrayType(type("t")))
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be referenced as a type variable");

        // a parameterless type that does not shadow a declared variable is fine
        Signature.builder()
                .typeVariable("T")
                .returnType(typeVariable("T"))
                .argumentType(type("bigint"))
                .argumentType(typeVariable("T"))
                .build();
    }

    @Test
    public void testVariableDeclarationOrderDoesNotMatter()
    {
        // build() canonicalizes the declaration order, so signatures declaring the same variables in a
        // different order are equal AND render identically (FunctionId derives from the rendering)
        Signature keyFirst = Signature.builder()
                .typeVariable("K")
                .typeVariable("V")
                .numericVariable("p")
                .returnType(mapType(typeVariable("K"), typeVariable("V")))
                .argumentType(typeVariable("K"))
                .argumentType(typeVariable("V"))
                .build();
        Signature valueFirst = Signature.builder()
                .numericVariable("p")
                .typeVariable("V")
                .typeVariable("K")
                .returnType(mapType(typeVariable("K"), typeVariable("V")))
                .argumentType(typeVariable("K"))
                .argumentType(typeVariable("V"))
                .build();

        assertThat(keyFirst).isEqualTo(valueFirst);
        assertThat(keyFirst.hashCode()).isEqualTo(valueFirst.hashCode());
        assertThat(keyFirst.toString()).isEqualTo(valueFirst.toString());
    }

    @Test
    public void testEqualityIncludesArgumentNames()
    {
        Signature withName = Signature.builder()
                .returnType(BIGINT)
                .argumentType(BIGINT.getTypeDescriptor(), "x")
                .build();
        Signature withoutName = Signature.builder()
                .returnType(BIGINT)
                .argumentType(BIGINT)
                .build();
        assertThat(withName).isNotEqualTo(withoutName);
    }
}
