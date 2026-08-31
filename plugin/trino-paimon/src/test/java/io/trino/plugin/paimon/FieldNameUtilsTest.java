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
package io.trino.plugin.paimon;

import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class FieldNameUtilsTest
{
    @Test
    public void testFieldNamesAreLowerCased()
    {
        RowType rowType = DataTypes.ROW(
                DataTypes.FIELD(0, "ID", DataTypes.INT()),
                DataTypes.FIELD(1, "Name", DataTypes.STRING()));

        assertThat(FieldNameUtils.fieldNames(rowType))
                .containsExactly("id", "name");
        assertThat(FieldNameUtils.fieldNameIndexes(rowType))
                .containsExactly(
                        Map.entry("id", 0),
                        Map.entry("name", 1));
    }

    @Test
    public void testFieldNamesRejectMalformedRows()
    {
        assertThatThrownBy(() -> FieldNameUtils.fieldNames(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("rowType is null");
        assertThatThrownBy(() -> FieldNameUtils.fieldNameIndexes(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("rowType is null");

        RowType duplicateRowType = DataTypes.ROW(
                DataTypes.FIELD(0, "ID", DataTypes.INT()),
                DataTypes.FIELD(1, "id", DataTypes.STRING()));
        assertThatThrownBy(() -> FieldNameUtils.fieldNames(duplicateRowType))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon row type contains case-insensitive duplicate field name 'id'");
        assertThatThrownBy(() -> FieldNameUtils.fieldNameIndexes(duplicateRowType))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Paimon row type contains case-insensitive duplicate field name 'id'");
    }

    @Test
    public void testToLowerCaseRejectsMalformedInputs()
    {
        assertThat(FieldNameUtils.toLowerCase("ID")).isEqualTo("id");
        assertThat(FieldNameUtils.toLowerCase(List.of("ID", "Name")))
                .containsExactly("id", "name");

        assertThatThrownBy(() -> FieldNameUtils.toLowerCase((String) null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("fieldName is null");
        assertThatThrownBy(() -> FieldNameUtils.toLowerCase((List<String>) null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("fieldNames is null");
        assertThatThrownBy(() -> FieldNameUtils.toLowerCase(Arrays.asList("id", null)))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("fieldName is null");
    }
}
