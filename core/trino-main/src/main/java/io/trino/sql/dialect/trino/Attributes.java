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
import io.trino.spi.type.Type;
import io.trino.sql.ir.Logical;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class Attributes
{
    public static final AttributeMetadata<Long> CARDINALITY = new AttributeMetadata<>("cardinality", Long.class, true);
    public static final AttributeMetadata<ConstantResult> CONSTANT_RESULT = new AttributeMetadata<>("constant_result", ConstantResult.class, true);
    public static final AttributeMetadata<String> FIELD_NAME = new AttributeMetadata<>("field_name", String.class, false);
    public static final AttributeMetadata<JoinType> JOIN_TYPE = new AttributeMetadata<>("join_type", JoinType.class, false);
    public static final AttributeMetadata<LogicalOperator> LOGICAL_OPERATOR = new AttributeMetadata<>("logical_operator", LogicalOperator.class, false);
    public static final AttributeMetadata<OutputNames> OUTPUT_NAMES = new AttributeMetadata<>("output_names", OutputNames.class, false);

    // TODO define attributes for deeply nested fields, not just top level or column level

    private Attributes() {}

    public static class AttributeMetadata<T>
    {
        private final String name;
        private final Class<T> type;
        private final boolean external;

        private AttributeMetadata(String name, Class<T> type, boolean external)
        {
            this.name = requireNonNull(name, "name is null");
            this.type = requireNonNull(type, "type is null");
            this.external = external;
        }

        public T getAttribute(Map<String, Object> map)
        {
            return this.type.cast(map.get(this.name));
        }

        public T putAttribute(Map<String, Object> map, T attribute)
        {
            return this.type.cast(map.put(name, attribute));
        }

        public Map<String, Object> asMap(T attribute)
        {
            return ImmutableMap.of(name, attribute);
        }
    }

    public record ConstantResult(Type type, Object value)
    {
        public ConstantResult
        {
            requireNonNull(type, "type is null");
        }

        @Override
        public String toString()
        {
            return value.toString() + ":" + type.toString();
        }
    }

    public enum JoinType
    {
        INNER,
        LEFT,
        RIGHT,
        FULL;

        public static JoinType of(io.trino.sql.planner.plan.JoinType joinType)
        {
            return switch (joinType) {
                case INNER -> INNER;
                case LEFT -> LEFT;
                case RIGHT -> RIGHT;
                case FULL -> FULL;
            };
        }
    }

    public record OutputNames(List<String> outputNames)
    {
        public OutputNames(List<String> outputNames)
        {
            this.outputNames = ImmutableList.copyOf(requireNonNull(outputNames, "outputNames is null"));
        }

        @Override
        public String toString()
        {
            return outputNames.toString();
        }
    }

    public enum LogicalOperator
    {
        AND,
        OR;

        public static LogicalOperator of(Logical.Operator operator)
        {
            return switch (operator) {
                case AND -> AND;
                case OR -> OR;
            };
        }
    }
}
