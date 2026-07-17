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

import io.trino.testing.QueryRunner;
import io.trino.testing.datatype.ColumnSetup;
import io.trino.testing.datatype.DataSetup;
import io.trino.testing.sql.TemporaryRelation;

import java.util.List;

import static io.trino.testing.assertions.Assert.assertEventually;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;

public class BigQueryDataSetup
        implements DataSetup
{
    private final QueryRunner queryRunner;
    private final DataSetup delegate;

    public BigQueryDataSetup(QueryRunner queryRunner, DataSetup delegate)
    {
        this.queryRunner = requireNonNull(queryRunner, "queryRunner is null");
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public TemporaryRelation setupTemporaryRelation(List<ColumnSetup> inputs)
    {
        TemporaryRelation relation = delegate.setupTemporaryRelation(inputs);
        waitForTableVisibility(relation.getName());
        return relation;
    }

    private void waitForTableVisibility(String tableName)
    {
        assertEventually(() -> assertThat(queryRunner.execute("DESCRIBE " + tableName).getRowCount()).isPositive());
    }
}
