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

import io.trino.spi.type.NamedTypeSignature;
import io.trino.spi.type.RowFieldName;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeParameter;
import io.trino.spi.type.TypeSignature;
import io.trino.spi.type.TypeSignatureParameter;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.StandardTypes.ROW;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.testng.Assert.assertEquals;

public class TestRowParametricType
{
    @Test
    public void testTypeSignatureRoundTrip()
    {
        TypeSignature typeSignature = new TypeSignature(
                ROW,
                TypeSignatureParameter.namedTypeParameter(new NamedTypeSignature(Optional.of(new RowFieldName("col1")), BIGINT.getTypeSignature())),
                TypeSignatureParameter.namedTypeParameter(new NamedTypeSignature(Optional.of(new RowFieldName("col2")), DOUBLE.getTypeSignature())));
        List<TypeParameter> parameters = typeSignature.getParameters().stream()
                .map(parameter -> TypeParameter.of(parameter, TESTING_TYPE_MANAGER))
                .collect(Collectors.toList());
        Type rowType = RowParametricType.ROW.createType(TESTING_TYPE_MANAGER, parameters);

        assertEquals(rowType.getTypeSignature(), typeSignature);
    }
}
