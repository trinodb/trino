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

import io.trino.spi.eventlistener.ColumnMaskReferenceInfo;
import io.trino.spi.eventlistener.FilterMaskReferenceInfo;
import org.assertj.core.api.AbstractAssert;

import static org.assertj.core.api.Assertions.assertThat;

public class FilterMaskReferenceInfoAssert
        extends AbstractAssert<FilterMaskReferenceInfoAssert, FilterMaskReferenceInfo>
{
    FilterMaskReferenceInfoAssert(FilterMaskReferenceInfo actual)
    {
        super(actual, FilterMaskReferenceInfoAssert.class);
    }

    public FilterMaskReferenceInfoAssert hasExpression(String expression)
    {
        assertThat(actual.expression()).isEqualToIgnoringWhitespace(expression);
        return this;
    }

    public FilterMaskReferenceInfoAssert hasTargetCatalogSchemaTable(String catalogName, String schemaName, String tableName)
    {
        assertThat(actual.targetCatalogName()).isEqualTo(catalogName);
        assertThat(actual.targetSchemaName()).isEqualTo(schemaName);
        assertThat(actual.targetTableName()).isEqualTo(tableName);
        return this;
    }

    public FilterMaskReferenceInfoAssert hasTargetColumn(String maskedColumnName)
    {
        TrinoAssertions.assertThat(actual).isInstanceOfSatisfying(
                ColumnMaskReferenceInfo.class,
                columnMaskInfo -> assertThat(columnMaskInfo.targetColumnName()).isEqualTo(maskedColumnName));
        return this;
    }
}
