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

import static org.testng.Assert.assertEquals;

public abstract class BaseKuduWithStandardInferSchemaConnectorSmokeTest
        extends BaseKuduConnectorSmokeTest
{
    @Override
    protected Optional<String> getKuduSchemaEmulationPrefix()
    {
        return Optional.of("presto::");
    }

    @Test
    public void testListingOfTableForDefaultSchema()
    {
        // The special $schemas table is created when listing schema names with schema emulation enabled
        // Depending on test ordering, this table may or may not be created when this test runs, so filter it out
        assertEquals(computeActual("SHOW TABLES FROM default LIKE '%$schemas'").getRowCount(), 0);
    }
}
