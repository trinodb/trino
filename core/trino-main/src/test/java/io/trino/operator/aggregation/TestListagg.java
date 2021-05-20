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
package io.trino.operator.aggregation;

import io.trino.metadata.Metadata;
import io.trino.sql.tree.QualifiedName;
import org.testng.annotations.Test;

import java.util.Collections;

import static io.trino.block.BlockAssertions.createBooleansBlock;
import static io.trino.block.BlockAssertions.createStringsBlock;
import static io.trino.metadata.MetadataManager.createTestMetadataManager;
import static io.trino.operator.aggregation.AggregationTestUtils.assertAggregation;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;

public class TestListagg
{
    private static final Metadata metadata = createTestMetadataManager();
    private static final InternalAggregationFunction listagg = metadata.getAggregateFunctionImplementation(
            metadata.resolveFunction(
                    QualifiedName.of("listagg"),
                    fromTypes(VARCHAR, VARCHAR, BOOLEAN, VARCHAR, BOOLEAN)));

    @Test
    public void testEmpty()
    {
        assertAggregation(
                listagg,
                null,
                createStringsBlock(new String[] {null}),
                createStringsBlock(","),
                createBooleansBlock(true),
                createStringsBlock("..."),
                createBooleansBlock(false));
    }

    @Test
    public void testOnlyNullValues()
    {
        assertAggregation(
                listagg,
                null,
                createStringsBlock(null, null, null),
                createStringsBlock(Collections.nCopies(3, ",")),
                createBooleansBlock(Collections.nCopies(3, true)),
                createStringsBlock(Collections.nCopies(3, "...")),
                createBooleansBlock(Collections.nCopies(3, true)));
    }

    @Test
    public void testOneValue()
    {
        assertAggregation(
                listagg,
                "value",
                createStringsBlock("value"),
                createStringsBlock(","),
                createBooleansBlock(true),
                createStringsBlock("..."),
                createBooleansBlock(false));
    }

    @Test
    public void testTwoValues()
    {
        assertAggregation(
                listagg,
                "value1,value2",
                createStringsBlock("value1", "value2"),
                createStringsBlock(Collections.nCopies(2, ",")),
                createBooleansBlock(Collections.nCopies(2, true)),
                createStringsBlock(Collections.nCopies(2, "...")),
                createBooleansBlock(Collections.nCopies(2, true)));
    }

    @Test
    public void testTwoValuesMixedWithNullValues()
    {
        assertAggregation(
                listagg,
                "value1,value2",
                createStringsBlock(null, "value1", null, "value2", null),
                createStringsBlock(Collections.nCopies(5, ",")),
                createBooleansBlock(Collections.nCopies(5, true)),
                createStringsBlock(Collections.nCopies(5, "...")),
                createBooleansBlock(Collections.nCopies(5, true)));
    }

    @Test
    public void testTwoValuesWithDefaultDelimiter()
    {
        assertAggregation(
                listagg,
                "value1value2",
                createStringsBlock("value1", "value2"),
                createStringsBlock(Collections.nCopies(2, "")),
                createBooleansBlock(Collections.nCopies(2, true)),
                createStringsBlock(Collections.nCopies(2, "...")),
                createBooleansBlock(Collections.nCopies(2, true)));
    }
}
