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
import io.airlift.slice.Slices;
import io.trino.spi.type.Type;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;

import java.util.List;

import static io.trino.plugin.geospatial.GeometryType.GEOMETRY;
import static io.trino.plugin.geospatial.SphericalGeographyType.SPHERICAL_GEOGRAPHY;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;

public abstract class AbstractTestExtractSpatial
        extends BaseRuleTest
{
    public AbstractTestExtractSpatial()
    {
        super(new GeoPlugin());
    }

    protected Call containsCall(Expression left, Expression right)
    {
        return functionCall("st_contains", ImmutableList.of(GEOMETRY, GEOMETRY), ImmutableList.of(left, right));
    }

    protected Call distanceCall(Expression left, Expression right)
    {
        return functionCall("st_distance", ImmutableList.of(GEOMETRY, GEOMETRY), ImmutableList.of(left, right));
    }

    protected Call sphericalDistanceCall(Expression left, Expression right)
    {
        return functionCall("st_distance", ImmutableList.of(SPHERICAL_GEOGRAPHY, SPHERICAL_GEOGRAPHY), ImmutableList.of(left, right));
    }

    protected Call geometryFromTextCall(Symbol symbol)
    {
        return functionCall("st_geometryfromtext", ImmutableList.of(VARCHAR), ImmutableList.of(symbol.toSymbolReference()));
    }

    protected Call geometryFromTextCall(String text)
    {
        return functionCall("st_geometryfromtext", ImmutableList.of(VARCHAR), ImmutableList.of(new Constant(VARCHAR, Slices.utf8Slice(text))));
    }

    protected Call toSphericalGeographyCall(Symbol symbol)
    {
        return functionCall("to_spherical_geography", ImmutableList.of(GEOMETRY), ImmutableList.of(geometryFromTextCall(symbol)));
    }

    protected Call toPointCall(Expression x, Expression y)
    {
        return functionCall("st_point", ImmutableList.of(DOUBLE, DOUBLE), ImmutableList.of(x, y));
    }

    private Call functionCall(String name, List<Type> types, List<Expression> arguments)
    {
        return new Call(tester().getMetadata().resolveBuiltinFunction(name, fromTypes(types)), arguments);
    }
}
