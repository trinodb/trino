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
package io.trino.lib;

import io.trino.spi.type.TypeDescriptor;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.TypeParameter.typeParameter;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.weakref.solver.Expression.apply;
import static org.weakref.solver.Expression.literal;
import static org.weakref.solver.Expression.symbol;

class TypeBridgeTest
{
    @Test
    void testUnknownParametricDescriptorKeepsParameters()
    {
        // A plugin-provided parametric type the bridge has no structural mapping for
        // (modelled on trino-ml's classifier(T)) must carry its parameters through
        // the descriptor instead of collapsing to a bare base symbol
        assertThat(TypeBridge.toExpression(new TypeDescriptor("Classifier", typeParameter(VARCHAR.getTypeDescriptor()))))
                .isEqualTo(apply("classifier", symbol("varchar")));

        // Numeric parameters of nested descriptors survive too
        assertThat(TypeBridge.toExpression(new TypeDescriptor("Classifier", typeParameter(createVarcharType(10).getTypeDescriptor()))))
                .isEqualTo(apply("classifier", apply("varchar", literal(10))));
    }

    @Test
    void testUnknownSimpleDescriptorMapsToBareSymbol()
    {
        assertThat(TypeBridge.toExpression(new TypeDescriptor("Model")))
                .isEqualTo(symbol("model"));
    }
}
