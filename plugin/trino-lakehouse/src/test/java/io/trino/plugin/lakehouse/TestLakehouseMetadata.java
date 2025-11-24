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
package io.trino.plugin.lakehouse;

import com.google.common.collect.ImmutableSet;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.JoinStatistics;
import io.trino.spi.connector.JoinType;
import io.trino.spi.connector.SaveMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionId;
import io.trino.spi.function.SchemaFunctionName;
import io.trino.spi.function.table.ConnectorTableFunctionHandle;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.TrinoPrincipal;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.testing.InterfaceTestUtils.assertAllMethodsOverridden;

public class TestLakehouseMetadata
{
    private static final Set<Method> EXCLUSIONS;

    static {
        try {
            EXCLUSIONS = ImmutableSet.<Method>builder()
                    .add(ConnectorMetadata.class.getMethod("supportsMissingColumnsOnInsert"))
                    .add(ConnectorMetadata.class.getMethod("refreshMaterializedView", ConnectorSession.class, SchemaTableName.class))
                    .add(ConnectorMetadata.class.getMethod("resolveIndex", ConnectorSession.class, ConnectorTableHandle.class, Set.class, Set.class, TupleDomain.class))
                    .add(ConnectorMetadata.class.getMethod("listFunctions", ConnectorSession.class, String.class))
                    .add(ConnectorMetadata.class.getMethod("getFunctions", ConnectorSession.class, SchemaFunctionName.class))
                    .add(ConnectorMetadata.class.getMethod("getFunctionMetadata", ConnectorSession.class, FunctionId.class))
                    .add(ConnectorMetadata.class.getMethod("getAggregationFunctionMetadata", ConnectorSession.class, FunctionId.class))
                    .add(ConnectorMetadata.class.getMethod("getFunctionDependencies", ConnectorSession.class, FunctionId.class, BoundSignature.class))
                    .add(ConnectorMetadata.class.getMethod("applyJoin", ConnectorSession.class, JoinType.class, ConnectorTableHandle.class, ConnectorTableHandle.class, ConnectorExpression.class, Map.class, Map.class, JoinStatistics.class))
                    .add(ConnectorMetadata.class.getMethod("applyJoin", ConnectorSession.class, JoinType.class, ConnectorTableHandle.class, ConnectorTableHandle.class, List.class, Map.class, Map.class, JoinStatistics.class))
                    .add(ConnectorMetadata.class.getMethod("applyTableFunction", ConnectorSession.class, ConnectorTableFunctionHandle.class))
                    .add(ConnectorMetadata.class.getMethod("applyTableScanRedirect", ConnectorSession.class, ConnectorTableHandle.class))
                    .add(ConnectorMetadata.class.getMethod("redirectTable", ConnectorSession.class, SchemaTableName.class))
                    .add(ConnectorMetadata.class.getMethod("getMaxWriterTasks", ConnectorSession.class))
                    .add(ConnectorMetadata.class.getMethod("createBranch", ConnectorSession.class, ConnectorTableHandle.class, String.class, Optional.class, SaveMode.class, Map.class))
                    .add(ConnectorMetadata.class.getMethod("dropBranch", ConnectorSession.class, ConnectorTableHandle.class, String.class))
                    .add(ConnectorMetadata.class.getMethod("fastForwardBranch", ConnectorSession.class, ConnectorTableHandle.class, String.class, String.class))
                    .add(ConnectorMetadata.class.getMethod("listBranches", ConnectorSession.class, SchemaTableName.class))
                    .add(ConnectorMetadata.class.getMethod("branchExists", ConnectorSession.class, SchemaTableName.class, String.class))
                    .add(ConnectorMetadata.class.getMethod("grantTableBranchPrivileges", ConnectorSession.class, SchemaTableName.class, String.class, Set.class, TrinoPrincipal.class, boolean.class))
                    .add(ConnectorMetadata.class.getMethod("denyTableBranchPrivileges", ConnectorSession.class, SchemaTableName.class, String.class, Set.class, TrinoPrincipal.class))
                    .add(ConnectorMetadata.class.getMethod("revokeTableBranchPrivileges", ConnectorSession.class, SchemaTableName.class, String.class, Set.class, TrinoPrincipal.class, boolean.class))
                    .build();
        }
        catch (NoSuchMethodException e) {
            throw new AssertionError(e);
        }
    }

    @Test
    void testMethodsImplemented()
    {
        assertAllMethodsOverridden(ConnectorMetadata.class, LakehouseMetadata.class, EXCLUSIONS);
    }
}
