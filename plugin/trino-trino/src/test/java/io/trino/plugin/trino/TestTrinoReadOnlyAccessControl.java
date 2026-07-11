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
package io.trino.plugin.trino;

import io.trino.spi.connector.SchemaRoutineName;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.AccessDeniedException;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestTrinoReadOnlyAccessControl
{
    private final TrinoReadOnlyAccessControl accessControl = new TrinoReadOnlyAccessControl();

    @Test
    void testAllowsOnlySystemQueryFunction()
    {
        assertThat(accessControl.canExecuteFunction(null, routine("system", "query"))).isTrue();
        assertThat(accessControl.canExecuteFunction(null, routine("other", "query"))).isFalse();
        assertThat(accessControl.canExecuteFunction(null, routine("system", "other"))).isFalse();

        assertThat(accessControl.canCreateViewWithExecuteFunction(null, routine("system", "query"))).isTrue();
        assertThat(accessControl.canCreateViewWithExecuteFunction(null, routine("other", "query"))).isFalse();
        assertThat(accessControl.canCreateViewWithExecuteFunction(null, routine("system", "other"))).isFalse();
    }

    @Test
    void testAllowsOnlyFlushMetadataCacheProcedure()
    {
        assertThatCode(() -> accessControl.checkCanExecuteProcedure(null, routine("system", "flush_metadata_cache")))
                .doesNotThrowAnyException();

        assertThatThrownBy(() -> accessControl.checkCanExecuteProcedure(null, routine("system", "execute")))
                .isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> accessControl.checkCanExecuteProcedure(null, routine("other", "flush_metadata_cache")))
                .isInstanceOf(AccessDeniedException.class);
        assertThatThrownBy(() -> accessControl.checkCanExecuteProcedure(null, routine("system", "other")))
                .isInstanceOf(AccessDeniedException.class);
    }

    @Test
    void testInheritsMutationDenials()
    {
        assertThatThrownBy(() -> accessControl.checkCanAlterColumn(null, new SchemaTableName("schema", "table")))
                .isInstanceOf(AccessDeniedException.class);
    }

    private static SchemaRoutineName routine(String schemaName, String routineName)
    {
        return new SchemaRoutineName(schemaName, routineName);
    }
}
