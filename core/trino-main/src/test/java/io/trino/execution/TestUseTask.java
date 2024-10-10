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
package io.trino.execution;

import io.trino.metadata.AbstractMockMetadata;
import io.trino.metadata.Metadata;
import io.trino.security.DenyAllAccessControl;
import io.trino.security.SecurityContext;
import io.trino.spi.security.AccessDeniedException;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.Use;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

@TestInstance(Lifecycle.PER_CLASS)
public class TestUseTask
{
    private static final Metadata METADATA = AbstractMockMetadata.dummyMetadata();

    private static final Use STATEMENT =
            new Use(Optional.of(new Identifier("identifier")), new Identifier("identifier"));

    private static final QueryStateMachine STATE_MACHINE = TestQueryStateMachine.createQueryStateMachine();

    @Test
    public void checkCatalogAccessBeforeMetadata()
    {
        UseTask task = new UseTask(METADATA, new DenyAllAccessControl());

        assertThatThrownBy(() -> task.execute(STATEMENT, STATE_MACHINE, null, null))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("Cannot access catalog");
    }

    @Test
    public void checkSchemaAccessBeforeMetadata()
    {
        class AllowCatalogAccessControl
                extends DenyAllAccessControl
        {
            @Override
            public Set<String> filterCatalogs(SecurityContext context, Set<String> catalogs)
            {
                return catalogs;
            }
        }

        UseTask task = new UseTask(METADATA, new AllowCatalogAccessControl());

        assertThatThrownBy(() -> task.execute(STATEMENT, STATE_MACHINE, null, null))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("Cannot access schema");
    }
}
