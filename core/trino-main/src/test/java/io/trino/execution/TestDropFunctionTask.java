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

import io.trino.Session;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.AbstractMockMetadata;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.security.AllowAllAccessControl;
import io.trino.sql.SqlEnvironmentConfig;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.DropFunction;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class TestDropFunctionTask
        extends BaseDataDefinitionTaskTest
{
    @Test
    void testLegacySignatureFallback()
    {
        testLookup(false);
        testLookup(true);
    }

    private void testLookup(boolean currentExists)
    {
        String current = "(interval day(2) to second(6))";
        String legacy = "(interval day to second)";
        List<String> lookups = new ArrayList<>();
        List<String> dropped = new ArrayList<>();
        Metadata functions = new AbstractMockMetadata()
        {
            @Override
            public boolean languageFunctionExists(Session session, QualifiedObjectName name, String signatureToken)
            {
                lookups.add(signatureToken);
                return signatureToken.equals(current) ? currentExists : signatureToken.equals(legacy);
            }

            @Override
            public void dropLanguageFunction(Session session, QualifiedObjectName name, String signatureToken)
            {
                dropped.add(signatureToken);
            }
        };
        DropFunction statement = (DropFunction) new SqlParser().createStatement("DROP FUNCTION catalog.schema.f(interval day to second)");
        new DropFunctionTask(new SqlEnvironmentConfig(), functions, new AllowAllAccessControl(), plannerContext.getLanguageFunctionManager())
                .execute(statement, queryStateMachine, List.of(), WarningCollector.NOOP);
        assertThat(lookups).isEqualTo(currentExists ? List.of(current) : List.of(current, legacy));
        assertThat(dropped).containsExactly(currentExists ? current : legacy);
    }
}
