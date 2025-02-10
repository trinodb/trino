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
package io.trino.plugin.bigquery;

import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedRow;

import static io.trino.testing.MaterializedResult.DEFAULT_PRECISION;
import static io.trino.testing.TestingProperties.requiredNonEmptySystemProperty;

abstract class BaseBigQueryProjectIdResolution
        extends AbstractTestQueryFramework
{
    protected final String projectId;
    protected final String parentProjectId;
    protected DistributedQueryRunner queryRunner;

    BaseBigQueryProjectIdResolution()
    {
        projectId = requiredNonEmptySystemProperty("testing.bigquery-project-id");
        parentProjectId = requiredNonEmptySystemProperty("testing.bigquery-parent-project-id");
    }

    protected AutoCloseable withSchema(String schemaName)
    {
        queryRunner.execute("DROP SCHEMA IF EXISTS " + schemaName);
        queryRunner.execute("CREATE SCHEMA " + schemaName);
        return () -> queryRunner.execute("DROP SCHEMA IF EXISTS " + schemaName);
    }

    protected static MaterializedRow row(String value)
    {
        return new MaterializedRow(DEFAULT_PRECISION, value);
    }
}
