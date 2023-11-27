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
package io.trino.plugin.geospatial;

import com.google.common.collect.ImmutableList;
import io.trino.spi.type.Type;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.StringLiteral;

import java.util.List;

import static io.trino.plugin.geospatial.GeometryType.GEOMETRY;
import static io.trino.plugin.geospatial.SphericalGeographyType.SPHERICAL_GEOGRAPHY;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;

public abstract class AbstractTestExtractSpatial
        extends BaseRuleTest
{
    public AbstractTestExtractSpatial()
    {
        super(new GeoPlugin());
    }

    protected FunctionCall containsCall(Expression left, Expression right)
    {
        return functionCall("st_contains", ImmutableList.of(GEOMETRY, GEOMETRY), ImmutableList.of(left, right));
    }

    protected FunctionCall distanceCall(Expression left, Expression right)
    {
        return functionCall("st_distance", ImmutableList.of(GEOMETRY, GEOMETRY), ImmutableList.of(left, right));
    }

    protected FunctionCall sphericalDistanceCall(Expression left, Expression right)
    {
        return functionCall("st_distance", ImmutableList.of(SPHERICAL_GEOGRAPHY, SPHERICAL_GEOGRAPHY), ImmutableList.of(left, right));
    }

    protected FunctionCall geometryFromTextCall(Symbol symbol)
    {
        return functionCall("st_geometryfromtext", ImmutableList.of(VARCHAR), ImmutableList.of(symbol.toSymbolReference()));
    }

    protected FunctionCall geometryFromTextCall(String text)
    {
        return functionCall("st_geometryfromtext", ImmutableList.of(VARCHAR), ImmutableList.of(new StringLiteral(text)));
    }

    protected FunctionCall toSphericalGeographyCall(Symbol symbol)
    {
        return functionCall("to_spherical_geography", ImmutableList.of(GEOMETRY), ImmutableList.of(geometryFromTextCall(symbol)));
    }

    protected FunctionCall toPointCall(Expression x, Expression y)
    {
        return functionCall("st_point", ImmutableList.of(BIGINT, BIGINT), ImmutableList.of(x, y));
    }

    private FunctionCall functionCall(String name, List<Type> types, List<Expression> arguments)
    {
        return new FunctionCall(tester().getMetadata().resolveBuiltinFunction(name, fromTypes(types)).toQualifiedName(), arguments);
    }
}
