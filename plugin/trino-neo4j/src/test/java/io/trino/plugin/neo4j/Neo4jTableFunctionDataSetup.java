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
package io.trino.plugin.neo4j;

import io.trino.testing.datatype.ColumnSetup;
import io.trino.testing.datatype.DataSetup;
import io.trino.testing.sql.TemporaryRelation;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class Neo4jTableFunctionDataSetup
        implements DataSetup
{
    private Optional<String> cypherMatchFragment;

    public Neo4jTableFunctionDataSetup(Optional<String> cypherMatchFragment)
    {
        this.cypherMatchFragment = cypherMatchFragment;
    }

    @Override
    public TemporaryRelation setupTemporaryRelation(List<ColumnSetup> inputs)
    {
        return new Neo4jTableFunctionTable(cypherMatchFragment, inputs);
    }

    public class Neo4jTableFunctionTable
            implements TemporaryRelation
    {
        private List<ColumnSetup> inputs;
        private Optional<String> cypherMatchFragment;

        public Neo4jTableFunctionTable(Optional<String> cypherMatchFragment, List<ColumnSetup> inputs)
        {
            this.cypherMatchFragment = cypherMatchFragment;
            this.inputs = requireNonNull(inputs, "inputs are null");
        }

        /**
         * Unlike other data setups where the given column inputs are used to create a new table definition with data,
         * This neo4j setup, just returns the given columns as is
         * @return
         */
        @Override
        public String getName()
        {
            AtomicInteger index = new AtomicInteger();
            String returnString = this.inputs.stream().map(columnSetup ->
                    columnSetup.getInputLiteral() + " as col_" + index.getAndIncrement()).collect(Collectors.joining(","));
            return String.format("TABLE(neo4j.system.query(query => '%s return %s'))", cypherMatchFragment.orElse(""), returnString);
        }

        @Override
        public void close()
        {
            // nothing to close or drop, coz we are not creating a table
        }
    }
}
