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
package io.trino.sql;

import io.trino.spi.connector.CatalogSchemaName;
import io.trino.sql.parser.ParsingException;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestSqlPath
{
    @Test
    void empty()
    {
        assertThat(SqlPath.EMPTY_PATH.getRawPath()).isEmpty();
        assertThat(SqlPath.EMPTY_PATH.getPath()).containsExactly(new CatalogSchemaName("system", "builtin"));
    }

    @Test
    void parsing()
    {
        assertThat(SqlPath.buildPath("a.b", Optional.empty()).getPath())
                .containsExactly(new CatalogSchemaName("system", "builtin"), new CatalogSchemaName("a", "b"));
        assertThat(SqlPath.buildPath("a.b, c.d", Optional.empty()).getPath())
                .containsExactly(new CatalogSchemaName("system", "builtin"), new CatalogSchemaName("a", "b"), new CatalogSchemaName("c", "d"));
        assertThat(SqlPath.buildPath("y", Optional.of("x")).getPath())
                .containsExactly(new CatalogSchemaName("system", "builtin"), new CatalogSchemaName("x", "y"));
        assertThat(SqlPath.buildPath("y, z", Optional.of("x")).getPath())
                .containsExactly(new CatalogSchemaName("system", "builtin"), new CatalogSchemaName("x", "y"), new CatalogSchemaName("x", "z"));
        assertThat(SqlPath.buildPath("a.b, c.d", Optional.of("x")).getPath())
                .containsExactly(new CatalogSchemaName("system", "builtin"), new CatalogSchemaName("a", "b"), new CatalogSchemaName("c", "d"));
        assertThat(SqlPath.buildPath("a.b, y", Optional.of("x")).getPath())
                .containsExactly(new CatalogSchemaName("system", "builtin"), new CatalogSchemaName("a", "b"), new CatalogSchemaName("x", "y"));

        assertThat(SqlPath.buildPath("a.b,   c.d", Optional.empty()).getRawPath()).isEqualTo("a.b,   c.d");
    }

    @Test
    void invalidPath()
    {
        assertThatThrownBy(() -> SqlPath.buildPath("too.many.qualifiers", Optional.empty()))
                .isInstanceOf(ParsingException.class)
                .hasMessageMatching("\\Qline 1:9: mismatched input '.'. Expecting: ',', <EOF>\\E");
    }
}
