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

import com.google.common.collect.ImmutableList;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeNotFoundException;
import io.prestosql.spi.type.TypeSignature;
import org.testng.annotations.Test;

import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.spi.function.OperatorType.EQUAL;
import static io.prestosql.spi.function.OperatorType.GREATER_THAN;
import static io.prestosql.spi.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static io.prestosql.spi.function.OperatorType.HASH_CODE;
import static io.prestosql.spi.function.OperatorType.IS_DISTINCT_FROM;
import static io.prestosql.spi.function.OperatorType.LESS_THAN;
import static io.prestosql.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.prestosql.spi.function.OperatorType.NOT_EQUAL;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestTypeRegistry
{
    private final Metadata metadata = createTestMetadataManager();

    @Test
    public void testNonexistentType()
    {
        assertThatThrownBy(() -> metadata.getType(new TypeSignature("not a real type")))
                .isInstanceOf(TypeNotFoundException.class)
                .hasMessage("Unknown type: not a real type");
    }

    @Test
    public void testOperatorsImplemented()
    {
        for (Type type : metadata.getTypes()) {
            if (type.isComparable()) {
                metadata.resolveOperator(EQUAL, ImmutableList.of(type, type));
                metadata.resolveOperator(NOT_EQUAL, ImmutableList.of(type, type));
                metadata.resolveOperator(IS_DISTINCT_FROM, ImmutableList.of(type, type));
                metadata.resolveOperator(HASH_CODE, ImmutableList.of(type));
            }
            if (type.isOrderable()) {
                metadata.resolveOperator(LESS_THAN, ImmutableList.of(type, type));
                metadata.resolveOperator(LESS_THAN_OR_EQUAL, ImmutableList.of(type, type));
                metadata.resolveOperator(GREATER_THAN_OR_EQUAL, ImmutableList.of(type, type));
                metadata.resolveOperator(GREATER_THAN, ImmutableList.of(type, type));
            }
        }
    }
}
