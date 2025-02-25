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
package io.trino.sql.dialect.trino;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.trino.spi.TrinoException;
import io.trino.sql.dialect.trino.operation.Query;
import io.trino.sql.newir.Block;
import io.trino.sql.newir.Program;
import io.trino.sql.newir.SourceNode;
import io.trino.sql.newir.Value;
import io.trino.sql.planner.plan.OutputNode;
import io.trino.sql.planner.plan.PlanNode;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.spi.StandardErrorCode.IR_ERROR;

/**
 * ProgramBuilder builds a MLIR program from a PlanNode tree.
 * For now, it builds a program for a single query, and assumes that OutputNode is the root PlanNode.
 * In the future, we might support multiple statements.
 * The resulting program has the special Query operation as the top-level operation.
 * It encloses all query computations in one block.
 */
public class ProgramBuilder
{
    private ProgramBuilder() {}

    public static Program buildProgram(PlanNode root)
    {
        checkArgument(root instanceof OutputNode, "Expected root to be an OutputNode. Actual: " + root.getClass().getSimpleName());

        ValueNameAllocator nameAllocator = new ValueNameAllocator();
        ImmutableMap.Builder<Value, SourceNode> valueMapBuilder = ImmutableMap.builder();

        String queryResultName = nameAllocator.newName();
        Block.Builder queryBlock = new Block.Builder(Optional.of("^query"), ImmutableList.of());

        // for now, ignoring return value. Could be worth to remember it as the final terminal Operation in the Program.
        root.accept(new RelationalProgramBuilder(nameAllocator, valueMapBuilder), new Context(queryBlock));
        Query query = new Query(queryResultName, queryBlock.build());
        valueMapBuilder.put(query.result(), query);

        // verify if all values are mapped
        Set<String> allocatedValues = IntStream.range(0, nameAllocator.label)
                .mapToObj(index -> "%" + index)
                .collect(toImmutableSet());
        Map<Value, SourceNode> valueMap = valueMapBuilder.buildOrThrow();
        Set<String> mappedValues = valueMap.keySet().stream()
                .map(Value::name)
                .collect(toImmutableSet());
        if (!Sets.symmetricDifference(allocatedValues, mappedValues).isEmpty()) {
            throw new TrinoException(IR_ERROR, "allocated values differ from mapped values");
        }

        return new Program(query, valueMap);
    }

    public static class ValueNameAllocator
    {
        private int label;

        public ValueNameAllocator()
        {
            this(0);
        }

        public ValueNameAllocator(int initialLabel)
        {
            this.label = initialLabel;
        }

        public String newName()
        {
            return "%" + label++;
        }
    }
}
