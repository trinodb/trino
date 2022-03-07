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
package io.trino.plugin.kudu;

import org.testng.annotations.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

public abstract class AbstractKuduWithDisabledInferSchemaConnectorTest
        extends AbstractKuduConnectorTest
{
    @Override
    protected Optional<String> getKuduSchemaEmulationPrefix()
    {
        return Optional.empty();
    }

    @Test
    public void testListingOfTableForDefaultSchema()
    {
        assertQuery("SHOW TABLES FROM default", "VALUES ('customer'), ('nation'), ('orders'), ('region')");
    }

    @Test
    @Override
    public void testCreateSchema()
    {
        assertThatThrownBy(super::testCreateSchema)
                .hasMessage("Creating schema in Kudu connector not allowed if schema emulation is disabled.");
    }

    @Test
    @Override
    public void testRenameTableAcrossSchema()
    {
        assertThatThrownBy(super::testRenameTableAcrossSchema)
                .hasMessage("Creating schema in Kudu connector not allowed if schema emulation is disabled.");
    }

    @Test
    @Override
    public void testDropNonEmptySchema()
    {
        assertThatThrownBy(super::testDropNonEmptySchema)
                .hasMessage("Creating schema in Kudu connector not allowed if schema emulation is disabled.");
    }
}
