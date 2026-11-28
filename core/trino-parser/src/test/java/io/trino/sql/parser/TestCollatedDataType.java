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
package io.trino.sql.parser;

import io.trino.sql.tree.CollatedDataType;
import io.trino.sql.tree.DataType;
import io.trino.sql.tree.GenericDataType;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.NumericParameter;
import io.trino.sql.tree.TypeParameter;
import org.junit.jupiter.api.Test;

import static io.trino.sql.ExpressionFormatter.formatExpression;
import static org.assertj.core.api.Assertions.assertThat;

class TestCollatedDataType
{
    private final SqlParser parser = new SqlParser();

    @Test
    void testCollateClauseWrapsTheType()
    {
        // a COLLATE clause applies to the type it follows; the type itself knows nothing of it
        DataType type = parser.createType("varchar(10) COLLATE unicode_ci");
        assertThat(type).isInstanceOf(CollatedDataType.class);
        CollatedDataType collated = (CollatedDataType) type;
        assertThat(collated.getCollation()).isEqualTo(new Identifier("unicode_ci"));
        assertThat(collated.getType()).isInstanceOf(GenericDataType.class);
        assertThat(((GenericDataType) collated.getType()).getName()).isEqualTo(new Identifier("varchar"));
        assertThat(((GenericDataType) collated.getType()).getArguments()).hasSize(1).first().isInstanceOf(NumericParameter.class);
        assertThat(parser.createType("varchar")).isInstanceOf(GenericDataType.class);

        // the clause binds before an ARRAY suffix that follows it
        DataType array = parser.createType("varchar COLLATE unicode_ci ARRAY");
        assertThat(array).isInstanceOf(GenericDataType.class);
        assertThat(((TypeParameter) ((GenericDataType) array).getArguments().getFirst()).getValue()).isInstanceOf(CollatedDataType.class);
        assertThat(((GenericDataType) array).getName().getValue()).isEqualToIgnoringCase("array");

        // the grammar accepts the clause after any type; analysis restricts it to character strings
        assertThat(parser.createType("bigint COLLATE unicode_ci")).isInstanceOf(CollatedDataType.class);
    }

    @Test
    void testFormatting()
    {
        for (String sql : new String[] {
                "varchar(10) COLLATE unicode_ci",
                "varchar COLLATE unicode",
                "array(varchar COLLATE unicode_ci)",
                "ROW(name varchar COLLATE unicode_ci)",
                "varchar COLLATE \"is\"",
        }) {
            DataType type = parser.createType(sql);
            assertThat(formatExpression(type)).isEqualTo(sql);
            assertThat(parser.createType(formatExpression(type))).isEqualTo(type);
        }
    }
}
