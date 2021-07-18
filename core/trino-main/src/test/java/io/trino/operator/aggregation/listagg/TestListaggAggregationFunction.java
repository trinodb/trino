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
package io.trino.operator.aggregation.listagg;

import io.airlift.slice.Slice;
import io.trino.block.BlockAssertions;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.VariableWidthBlockBuilder;
import org.testcontainers.shaded.org.apache.commons.lang.StringUtils;
import org.testng.annotations.Test;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.block.BlockAssertions.createStringsBlock;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.StandardErrorCode.QUERY_REJECTED;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestListaggAggregationFunction
{
    @Test
    public void testInputEmptyState()
    {
        SingleListaggAggregationState state = new SingleListaggAggregationState(VARCHAR);

        String s = "value1";
        Block value = createStringsBlock(s);
        Slice separator = utf8Slice(",");
        Slice overflowTruncationFiller = utf8Slice("...");
        ListaggAggregationFunction.input(VARCHAR,
                state,
                value,
                separator,
                false,
                overflowTruncationFiller,
                true,
                0);

        assertFalse(state.isEmpty());
        assertEquals(state.getSeparator(), separator);
        assertFalse(state.isOverflowError());
        assertEquals(state.getOverflowTruncationFiller(), overflowTruncationFiller);
        assertTrue(state.isOverflowTruncationCountIndication());

        BlockBuilder out = new VariableWidthBlockBuilder(null, 16, 128);
        state.forEach((block, position) -> {
            block.writeBytesTo(position, 0, block.getSliceLength(position), out);
        });
        out.closeEntry();
        String result = (String) BlockAssertions.getOnlyValue(VARCHAR, out);
        assertEquals(result, s);
    }

    @Test
    public void testInputOverflowTruncationFillerTooLong()
    {
        String overflowTruncationFillerTooLong = StringUtils.repeat(".", 65_537);

        SingleListaggAggregationState state = new SingleListaggAggregationState(VARCHAR);

        assertThatThrownBy(() -> ListaggAggregationFunction.input(VARCHAR,
                state,
                createStringsBlock("value1"),
                utf8Slice(","),
                false,
                utf8Slice(overflowTruncationFillerTooLong),
                false,
                0))
                .isInstanceOf(TrinoException.class)
                .matches(throwable -> ((TrinoException) throwable).getErrorCode() == INVALID_FUNCTION_ARGUMENT.toErrorCode());
    }

    @Test
    public void testOutputStateEmpty()
    {
        SingleListaggAggregationState state = createListaggAggregationState(",", true, "...", false);
        state.add(createStringsBlock((String) null), 0);

        BlockBuilder out = new VariableWidthBlockBuilder(null, 16, 128);
        ListaggAggregationFunction.outputState(state, out, 1024);
        String result = (String) BlockAssertions.getOnlyValue(VARCHAR, out);
        assertNull(result);
    }

    @Test
    public void testOutputStateWithAllValuesNull()
    {
        SingleListaggAggregationState state = createListaggAggregationState(",", true, "...", false);
        state.add(createStringsBlock((String) null), 0);
        state.add(createStringsBlock((String) null), 0);
        state.add(createStringsBlock((String) null), 0);

        BlockBuilder out = new VariableWidthBlockBuilder(null, 16, 128);
        ListaggAggregationFunction.outputState(state, out, 1024);
        String result = (String) BlockAssertions.getOnlyValue(VARCHAR, out);
        assertEquals(result, null);
    }

    @Test
    public void testOutputStateWithNullValues()
    {
        SingleListaggAggregationState state = createListaggAggregationState(",", true, "...", false);
        state.add(createStringsBlock((String) null), 0);
        state.add(createStringsBlock("value1"), 0);
        state.add(createStringsBlock((String) null), 0);
        state.add(createStringsBlock("value2"), 0);

        BlockBuilder out = new VariableWidthBlockBuilder(null, 16, 128);
        ListaggAggregationFunction.outputState(state, out, 1024);
        String result = (String) BlockAssertions.getOnlyValue(VARCHAR, out);
        assertEquals(result, "value1,value2");
    }

    @Test
    public void testOutputStateSingleValue()
    {
        SingleListaggAggregationState state = createListaggAggregationState(",", true, "...", false,
                "value1");

        BlockBuilder out = new VariableWidthBlockBuilder(null, 16, 128);
        ListaggAggregationFunction.outputState(state, out, 1024);
        String result = (String) BlockAssertions.getOnlyValue(VARCHAR, out);
        assertEquals(result, "value1");
    }

    @Test
    public void testOutputStateWithOverflowError()
    {
        SingleListaggAggregationState state = createListaggAggregationState("", true, "...", false,
                "value1", "value2");

        BlockBuilder out = new VariableWidthBlockBuilder(null, 16, 128);
        assertThatThrownBy(() -> ListaggAggregationFunction.outputState(state, out, 10))
                .isInstanceOf(TrinoException.class)
                .matches(throwable -> ((TrinoException) throwable).getErrorCode() == QUERY_REJECTED.toErrorCode());
    }

    @Test
    public void testOutputStateWithEmptyDelimiter()
    {
        SingleListaggAggregationState state = createListaggAggregationState("", true, "...", false,
                "value1", "value2");

        BlockBuilder out = new VariableWidthBlockBuilder(null, 16, 128);
        ListaggAggregationFunction.outputState(state, out, 12);
        String result = (String) BlockAssertions.getOnlyValue(VARCHAR, out);
        assertEquals(result, "value1value2");
    }

    @Test
    public void testOutputTruncatedStateFirstValueTooBigWithoutIndicationCount()
    {
        SingleListaggAggregationState state = createListaggAggregationState(",", false, "...", false,
                "value1", "value2");

        BlockBuilder out = new VariableWidthBlockBuilder(null, 16, 128);
        ListaggAggregationFunction.outputState(state, out, 5);
        String result = (String) BlockAssertions.getOnlyValue(VARCHAR, out);
        assertEquals(result, "va...");
    }

    @Test
    public void testOutputTruncatedStateLastDelimiterOmitted()
    {
        SingleListaggAggregationState state = createListaggAggregationState("###", false, "...", false,
                "value1", "value2", "value3");

        BlockBuilder out = new VariableWidthBlockBuilder(null, 16, 128);
        ListaggAggregationFunction.outputState(state, out, 20);
        String result = (String) BlockAssertions.getOnlyValue(VARCHAR, out);
        assertEquals(result, "value1###value2...");
    }

    @Test
    public void testOutputTruncatedStateWithoutIndicationCount()
    {
        SingleListaggAggregationState state = createListaggAggregationState(",", false, "...", false,
                "value1", "value2");

        BlockBuilder out = new VariableWidthBlockBuilder(null, 16, 128);
        ListaggAggregationFunction.outputState(state, out, 11);
        String result = (String) BlockAssertions.getOnlyValue(VARCHAR, out);
        assertEquals(result, "value1,v...");
    }

    @Test
    public void testOutputTruncatedStateWithIndicationCount()
    {
        SingleListaggAggregationState state = createListaggAggregationState(",", false, "...", true,
                "value1", "value2");

        BlockBuilder out = new VariableWidthBlockBuilder(null, 16, 128);
        ListaggAggregationFunction.outputState(state, out, 11);
        String result = (String) BlockAssertions.getOnlyValue(VARCHAR, out);
        assertEquals(result, "value...(1)");
    }

    @Test
    public void testOutputTruncatedStateWithIndicationCountShouldApplyOnlyForNonNullValues()
    {
        SingleListaggAggregationState state = createListaggAggregationState(",", false, "...", true,
                "value1", null, "value2", null, null, null, "value3", null);

        BlockBuilder out = new VariableWidthBlockBuilder(null, 16, 128);
        ListaggAggregationFunction.outputState(state, out, 12);
        String result = (String) BlockAssertions.getOnlyValue(VARCHAR, out);
        assertEquals(result, "value1...(2)");
    }

    @Test
    public void testOutputTruncatedStateWithIndicationCountShouldBeZeroWhenDealingWithOnlyNullValues()
    {
        SingleListaggAggregationState state = createListaggAggregationState(",", false, "...", true,
                "value1", "largevalue2", null, null, null, null);

        BlockBuilder out = new VariableWidthBlockBuilder(null, 16, 128);
        ListaggAggregationFunction.outputState(state, out, 15);
        String result = (String) BlockAssertions.getOnlyValue(VARCHAR, out);
        assertEquals(result, "value1,la...(0)");
    }

    @Test
    public void testOutputTruncatedStateWithIndicationCountTwoCharacters()
    {
        SingleListaggAggregationState state = createListaggAggregationState(",", false, "...", true,
                "a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l");

        BlockBuilder out = new VariableWidthBlockBuilder(null, 16, 128);
        ListaggAggregationFunction.outputState(state, out, 10);
        String result = (String) BlockAssertions.getOnlyValue(VARCHAR, out);
        assertEquals(result, "a,b...(10)");
    }

    @Test
    public void testOutputTruncatedStateWithIndicationCountOneCharacter()
    {
        SingleListaggAggregationState state = createListaggAggregationState(",", false, "...", true,
                "a", "b", "ccc", "d", "e", "f", "g", "h", "i", "j", "k", "l");

        BlockBuilder out = new VariableWidthBlockBuilder(null, 16, 128);
        ListaggAggregationFunction.outputState(state, out, 11);
        String result = (String) BlockAssertions.getOnlyValue(VARCHAR, out);
        assertEquals(result, "a,b,c...(9)");
    }

    private static SingleListaggAggregationState createListaggAggregationState(String separator, boolean overflowError, String truncationFiller, boolean truncationCountIndication, String... values)
    {
        SingleListaggAggregationState state = new SingleListaggAggregationState(VARCHAR);
        state.setSeparator(utf8Slice(separator));
        state.setOverflowError(overflowError);
        state.setOverflowTruncationFiller(utf8Slice(truncationFiller));
        state.setOverflowTruncationCountIndication(truncationCountIndication);
        for (String value : values) {
            state.add(createStringsBlock(value), 0);
        }
        return state;
    }
}
