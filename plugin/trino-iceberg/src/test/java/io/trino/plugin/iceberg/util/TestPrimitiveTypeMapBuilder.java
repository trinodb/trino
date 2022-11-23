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
package io.trino.plugin.iceberg.util;

import com.google.common.collect.ImmutableList;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.IntegerType.INTEGER;
import static org.assertj.core.api.Assertions.assertThat;

public class TestPrimitiveTypeMapBuilder
{
    @Test
    public void testMakeTypeMapProducesCorrectMapForTypesWithQuotes()
    {
        List<Type> inputTypes = ImmutableList.of(INTEGER, RowType.from(ImmutableList.of(new RowType.Field(Optional.of("another identifier"), INTEGER))));
        List<String> inputColumnNames = ImmutableList.of("an identifier with \"quotes\" ", "a");

        assertThat(PrimitiveTypeMapBuilder.makeTypeMap(inputTypes, inputColumnNames)).containsExactly(
                Map.entry(ImmutableList.of("an_x20identifier_x20with_x20_x22quotes_x22_x20"), INTEGER),
                Map.entry(ImmutableList.of("a", "another_x20identifier"), INTEGER));
    }
}
