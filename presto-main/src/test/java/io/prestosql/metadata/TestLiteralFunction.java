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
package io.prestosql.metadata;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.sql.tree.QualifiedName;
import org.testng.annotations.Test;

import static io.prestosql.metadata.LiteralFunction.getLiteralFunctionSignature;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static io.prestosql.sql.analyzer.TypeSignatureProvider.fromTypeSignatures;
import static org.testng.Assert.assertEquals;

public class TestLiteralFunction
{
    @Test
    public void testLiteralFunction()
    {
        Signature signature = getLiteralFunctionSignature(TIMESTAMP_WITH_TIME_ZONE);
        assertEquals(signature.getName(), "$literal$timestamp with time zone");
        assertEquals(signature.getArgumentTypes(), ImmutableList.of(BIGINT.getTypeSignature()));
        assertEquals(signature.getReturnType().getBase(), StandardTypes.TIMESTAMP_WITH_TIME_ZONE);

        Signature function = createTestMetadataManager().resolveFunction(QualifiedName.of(signature.getName()), fromTypeSignatures(signature.getArgumentTypes()));
        assertEquals(function.getArgumentTypes(), ImmutableList.of(BIGINT.getTypeSignature()));
        assertEquals(signature.getReturnType().getBase(), StandardTypes.TIMESTAMP_WITH_TIME_ZONE);
    }
}
