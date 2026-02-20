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
package io.trino.tests.product.deltalake;

import io.trino.tempto.query.QueryResult;
import io.trino.tests.product.deltalake.util.DatabricksVersion;
import io.trino.tests.product.deltalake.util.DeltaLakeTestUtils;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public final class DeltaLakeDatabricksUtilsJunit
{
    public static final String DATABRICKS_COMMUNICATION_FAILURE_ISSUE = DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_ISSUE;
    public static final String DATABRICKS_COMMUNICATION_FAILURE_MATCH = DeltaLakeTestUtils.DATABRICKS_COMMUNICATION_FAILURE_MATCH;

    private DeltaLakeDatabricksUtilsJunit() {}

    public static QueryResult dropDeltaTableWithRetry(DeltaLakeDatabricksEnvironment env, String tableName)
    {
        requireNonNull(env, "env is null");
        return DeltaLakeTestUtils.dropDeltaTableWithRetry(tableName);
    }

    public static Optional<DatabricksVersion> getDatabricksRuntimeVersion(DeltaLakeDatabricksEnvironment env)
    {
        requireNonNull(env, "env is null");
        return DeltaLakeTestUtils.getDatabricksRuntimeVersion();
    }
}
