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
package io.trino.common.assertions;

import io.trino.spi.eventlistener.ColumnInfo;
import io.trino.spi.eventlistener.TableInfo;
import io.trino.spi.eventlistener.TableReferenceInfo;
import org.assertj.core.api.AbstractAssert;

import java.util.Optional;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

public class TableInfoAssert
        extends AbstractAssert<TableInfoAssert, TableInfo>
{
    TableInfoAssert(TableInfo actual)
    {
        super(actual, TableInfoAssert.class);
    }

    public TableInfoAssert hasCatalogSchemaTable(String expectedCatalog, String expectedSchema, String expectedTable)
    {
        assertThat(actual.getCatalog()).isEqualTo(expectedCatalog);
        assertThat(actual.getSchema()).isEqualTo(expectedSchema);
        assertThat(actual.getTable()).isEqualTo(expectedTable);
        return this;
    }

    public TableInfoAssert hasAuthorization(String expectedAuthorization)
    {
        assertThat(actual.getAuthorization()).isEqualTo(expectedAuthorization);
        return this;
    }

    public TableInfoAssert isNotDirectlyReferenced()
    {
        assertThat(actual.isDirectlyReferenced()).isFalse();
        return this;
    }

    public TableInfoAssert isDirectlyReferenced()
    {
        assertThat(actual.isDirectlyReferenced()).isTrue();
        return this;
    }

    public TableInfoAssert hasColumnsWithoutMasking(String... columnNames)
    {
        assertThat(actual.getColumns()).extracting(ColumnInfo::getColumn).containsExactly(columnNames);
        assertThat(actual.getColumns()).allSatisfy(columnInfo -> assertThat(columnInfo.getMask()).isEmpty());
        return this;
    }

    public TableInfoAssert hasColumnNames(String... columnNames)
    {
        assertThat(actual.getColumns()).extracting(ColumnInfo::getColumn).containsExactly(columnNames);
        return this;
    }

    /**
     * Asserts that the table has the given column masks. Use 'null' to represent an empty mask.
     */
    public TableInfoAssert hasColumnMasks(String... columnMasks)
    {
        assertThat(actual.getColumns()).hasSize(columnMasks.length);
        for (int i = 0; i < columnMasks.length; i++) {
            String expectedMask = columnMasks[i];
            Optional<String> actualMask = actual.getColumns().get(i).getMask();
            if (expectedMask == null) {
                assertThat(actualMask).isEmpty();
            }
            else {
                assertThat(actualMask).hasValueSatisfying(maskText ->
                        assertThat(maskText).isEqualToIgnoringWhitespace(expectedMask));
            }
        }
        return this;
    }

    public TableInfoAssert hasNoRowFilters()
    {
        assertThat(actual.getFilters()).isEmpty();
        return this;
    }

    public TableInfoAssert hasRowFilters(String... filterTexts)
    {
        assertThat(actual.getFilters()).hasSize(filterTexts.length);
        for (int i = 0; i < filterTexts.length; i++) {
            assertThat(actual.getFilters().get(i)).isEqualToIgnoringWhitespace(filterTexts[i]);
        }
        return this;
    }

    public TableInfoAssert hasViewText(String viewText)
    {
        assertThat(actual.getViewText()).hasValueSatisfying(sql -> assertThat(sql).isEqualToIgnoringWhitespace(viewText));
        return this;
    }

    public TableInfoAssert hasNoTableReferences()
    {
        assertThat(actual.getReferenceChain()).isEmpty();
        return this;
    }

    @SafeVarargs
    public final TableInfoAssert hasTableReferencesSatisfying(Consumer<TableReferenceInfo>... tableReferenceAssertions)
    {
        assertThat(actual.getReferenceChain()).hasSize(tableReferenceAssertions.length);
        for (int i = 0; i < tableReferenceAssertions.length; i++) {
            tableReferenceAssertions[i].accept(actual.getReferenceChain().get(i));
        }
        return this;
    }
}
