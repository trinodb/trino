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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.connector.CatalogServiceProvider;
import io.trino.execution.warnings.WarningCollector;
import io.trino.metadata.QualifiedObjectName;
import io.trino.metadata.TablePropertyManager;
import io.trino.security.AllowAllAccessControl;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.Property;
import io.trino.sql.tree.SetProperties;
import io.trino.sql.tree.StringLiteral;
import org.testng.annotations.Test;

import static io.trino.sql.tree.SetProperties.Type.MATERIALIZED_VIEW;
import static org.assertj.core.api.Assertions.assertThat;

@Test(singleThreaded = true)
public class TestSetPropertiesTask
        extends BaseDataDefinitionTaskTest
{
    @Test
    public void testSetMaterializedViewProperties()
    {
        QualifiedObjectName materializedViewName = qualifiedObjectName("test_materialized_view");
        metadata.createMaterializedView(testSession, materializedViewName, someMaterializedView(), false, false);

        // set all properties to non-DEFAULT values and check the results
        executeSetProperties(
                new SetProperties(
                        MATERIALIZED_VIEW,
                        asQualifiedName(materializedViewName),
                        ImmutableList.of(
                                new Property(new Identifier(MATERIALIZED_VIEW_PROPERTY_1_NAME), new LongLiteral("111")),
                                new Property(new Identifier(MATERIALIZED_VIEW_PROPERTY_2_NAME), new StringLiteral("abc")))));
        assertThat(metadata.getMaterializedView(testSession, materializedViewName).get().getProperties()).isEqualTo(
                ImmutableMap.of(
                        MATERIALIZED_VIEW_PROPERTY_1_NAME, 111L,
                        MATERIALIZED_VIEW_PROPERTY_2_NAME, "abc"));

        // set all properties to DEFAULT and check the results
        executeSetProperties(
                new SetProperties(
                        MATERIALIZED_VIEW,
                        asQualifiedName(materializedViewName),
                        ImmutableList.of(
                                new Property(new Identifier(MATERIALIZED_VIEW_PROPERTY_1_NAME)),
                                new Property(new Identifier(MATERIALIZED_VIEW_PROPERTY_2_NAME)))));
        // since the default value of property 1 is null, property 1 should not appear in the result, whereas property 2 should appear in
        // the result with its (non-null) default value
        assertThat(metadata.getMaterializedView(testSession, materializedViewName).get().getProperties()).isEqualTo(
                ImmutableMap.of(MATERIALIZED_VIEW_PROPERTY_2_NAME, MATERIALIZED_VIEW_PROPERTY_2_DEFAULT_VALUE));
    }

    private void executeSetProperties(SetProperties statement)
    {
        new SetPropertiesTask(plannerContext, new AllowAllAccessControl(), new TablePropertyManager(CatalogServiceProvider.fail()), materializedViewPropertyManager)
                .execute(statement, queryStateMachine, ImmutableList.of(), WarningCollector.NOOP);
    }
}
