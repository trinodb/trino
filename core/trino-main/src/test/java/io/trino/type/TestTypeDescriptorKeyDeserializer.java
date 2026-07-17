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
package io.trino.type;

import io.trino.spi.type.TypeDescriptor;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.TypeParameter.numericParameter;
import static io.trino.spi.type.VarcharType.UNBOUNDED_LENGTH;
import static org.assertj.core.api.Assertions.assertThat;

class TestTypeDescriptorKeyDeserializer
{
    @Test
    void testDeserializesInternalForm()
    {
        // A TypeDescriptor map key (e.g. ResolvedFunction.typeDependencies, shipped in plan fragments) is
        // serialized in the internal IR form produced by TypeDescriptor.toString(), which is not always
        // valid SQL: an unbounded varchar materializes its sentinel length, and a zoned/interval base is an
        // opaque token. The key deserializer must parse that IR structurally — like the value deserializer —
        // rather than through the SQL type parser, which would reject it.
        TypeDescriptor unboundedVarchar = new TypeDescriptor("varchar", numericParameter(UNBOUNDED_LENGTH));
        String serializedKey = unboundedVarchar.toString();
        assertThat(serializedKey).isEqualTo("varchar(2147483647)");
        assertThat(new TypeDescriptorKeyDeserializer().deserializeKey(serializedKey, null))
                .isEqualTo(unboundedVarchar);
    }
}
