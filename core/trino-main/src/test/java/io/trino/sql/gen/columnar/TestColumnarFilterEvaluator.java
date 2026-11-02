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
package io.trino.sql.gen.columnar;

import io.trino.operator.TestingSourcePage;
import io.trino.operator.project.InputChannels;
import io.trino.operator.project.SelectedPositions;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static io.trino.block.BlockAssertions.createLongSequenceBlock;
import static io.trino.operator.project.SelectedPositions.positionsList;
import static io.trino.operator.project.SelectedPositions.positionsRange;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static java.lang.System.arraycopy;
import static org.assertj.core.api.Assertions.assertThat;

public class TestColumnarFilterEvaluator
{
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testMaterializesOnlyFilterInputs(boolean list)
    {
        Block first = createLongSequenceBlock(0, 4);
        Block last = createLongSequenceBlock(100, 104);
        TestingSourcePage source = new TestingSourcePage(4, first, null, last);
        ColumnarFilter filter = new ColumnarFilter()
        {
            @Override
            public InputChannels getInputChannels()
            {
                return new InputChannels(2, 0);
            }

            @Override
            public int filterPositionsRange(ConnectorSession session, int[] outputPositions, int offset, int size, SourcePage loadedPage)
            {
                assertInputsLoaded(loadedPage);
                for (int index = 0; index < size; index++) {
                    outputPositions[index] = offset + index;
                }
                return size;
            }

            @Override
            public int filterPositionsList(ConnectorSession session, int[] outputPositions, int[] activePositions, int offset, int size, SourcePage loadedPage)
            {
                assertInputsLoaded(loadedPage);
                arraycopy(activePositions, offset, outputPositions, 0, size);
                return size;
            }

            private void assertInputsLoaded(SourcePage loadedPage)
            {
                assertThat(source.wasLoaded(0)).isTrue();
                assertThat(source.wasLoaded(1)).isFalse();
                assertThat(source.wasLoaded(2)).isTrue();
                assertThat(loadedPage.getPositionCount()).isEqualTo(4);
                assertThat(loadedPage.getChannelCount()).isEqualTo(2);
                assertThat(loadedPage.getBlock(0)).isSameAs(last);
                assertThat(loadedPage.getBlock(1)).isSameAs(first);
            }
        };
        SelectedPositions activePositions = list ? positionsList(new int[] {0, 1, 3}, 1, 2) : positionsRange(1, 2);
        SelectedPositions result = new ColumnarFilterEvaluator(filter).evaluate(SESSION, activePositions, source).selectedPositions();
        assertThat(result.size()).isEqualTo(2);
        assertThat(result.isList()).isEqualTo(list);
        if (list) {
            assertThat(result.getPositions()).containsExactly(1, 3);
        }
        else {
            assertThat(result.getOffset()).isEqualTo(1);
        }
    }
}
