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
package io.trino.plugin.varada.dal;

import io.trino.plugin.varada.type.VaradaTypeDeserializer;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.VarcharType;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public final class ColumnTypeUtil
{
    private static final TypeManager typeManager = mock(TypeManager.class);

    private ColumnTypeUtil() {}

    public static void initVaradaTypeDeserializer(Type... types)
    {
        for (Type type : types) {
            when(typeManager.getType(eq(type.getTypeSignature()))).thenReturn(type);
        }
    }

    static {
        when(typeManager.getType(eq(IntegerType.INTEGER.getTypeSignature()))).thenReturn(IntegerType.INTEGER);
        when(typeManager.getType(eq(BigintType.BIGINT.getTypeSignature()))).thenReturn(BigintType.BIGINT);
        when(typeManager.fromSqlType(startsWith("varchar"))).thenReturn(VarcharType.VARCHAR);

        new VaradaTypeDeserializer(typeManager);
    }
}
