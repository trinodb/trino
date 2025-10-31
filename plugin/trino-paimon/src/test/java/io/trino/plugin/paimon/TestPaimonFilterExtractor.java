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
package io.trino.plugin.paimon;

import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Constant;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.StandardFunctions;
import io.trino.spi.expression.Variable;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.Type;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static io.trino.plugin.paimon.PaimonFilterExtractor.TRINO_MAP_ELEMENT_AT_FUNCTION_NAME;
import static io.trino.plugin.paimon.PaimonFilterExtractor.extractTrinoColumnHandleForExpressionFilter;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.apache.paimon.fileindex.FileIndexCommon.toMapKey;
import static org.assertj.core.api.Assertions.assertThat;

final class TestPaimonFilterExtractor
{
    @Test
    void testExtractTrinoColumnHandleForExpressionFilter()
    {
        Type mapType = TESTING_TYPE_MANAGER.fromSqlType("map<varchar,varchar>");
        String columnName = "map";
        String mapKeyName = "key";
        String constantValue = "value";
        Slice value = Slices.utf8Slice(constantValue);
        Call elementAtFunction = new Call(BooleanType.BOOLEAN,
                new FunctionName(TRINO_MAP_ELEMENT_AT_FUNCTION_NAME),
                List.of(new Variable(columnName, mapType), new Constant(Slices.utf8Slice(mapKeyName), VARCHAR)));
        ConnectorExpression expression = new Call(
                BooleanType.BOOLEAN,
                StandardFunctions.EQUAL_OPERATOR_FUNCTION_NAME,
                List.of(elementAtFunction, new Constant(value, VARCHAR)));
        Map<String, ColumnHandle> assignments = ImmutableMap.of(columnName, PaimonColumnHandle.of(columnName, DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING()), 0));
        Constraint constraint = new Constraint(TupleDomain.all(), expression, assignments);

        Map<PaimonColumnHandle, Domain> domainMap = extractTrinoColumnHandleForExpressionFilter(constraint);
        assertThat(domainMap).hasSize(1);

        Map.Entry<PaimonColumnHandle, Domain> next = domainMap.entrySet().iterator().next();
        assertThat(next.getKey().columnName()).isEqualTo(toMapKey(columnName, mapKeyName));
        assertThat(
                next.getValue()
                        .getValues()
                        .getRanges()
                        .getOrderedRanges()
                        .get(0)
                        .getLowBoundedValue())
                .isEqualTo(value);
    }
}
