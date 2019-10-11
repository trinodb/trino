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
package io.prestosql.type;

import io.prestosql.spi.type.NamedTypeSignature;
import io.prestosql.spi.type.RowFieldName;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeParameter;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.spi.type.TypeSignatureParameter;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.StandardTypes.ROW;
import static io.prestosql.testing.assertions.Assert.assertEquals;

public class TestRowParametricType
{
    @Test
    public void testTypeSignatureRoundTrip()
    {
        TypeManager typeManager = new InternalTypeManager(createTestMetadataManager());
        TypeSignature typeSignature = new TypeSignature(
                ROW,
                TypeSignatureParameter.namedTypeParameter(new NamedTypeSignature(Optional.of(new RowFieldName("col1", false)), BIGINT.getTypeSignature())),
                TypeSignatureParameter.namedTypeParameter(new NamedTypeSignature(Optional.of(new RowFieldName("col2", true)), DOUBLE.getTypeSignature())));
        List<TypeParameter> parameters = typeSignature.getParameters().stream()
                .map(parameter -> TypeParameter.of(parameter, typeManager))
                .collect(Collectors.toList());
        Type rowType = RowParametricType.ROW.createType(typeManager, parameters);

        assertEquals(rowType.getTypeSignature(), typeSignature);
    }
}
