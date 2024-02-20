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

import com.google.common.collect.ImmutableMap;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.assertions.PlanMatchPattern;
import io.trino.sql.planner.iterative.rule.ExtractSpatialJoins;
import io.trino.sql.planner.iterative.rule.test.RuleBuilder;
import io.trino.sql.planner.iterative.rule.test.RuleTester;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.LogicalExpression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.NotExpression;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.geospatial.GeometryType.GEOMETRY;
import static io.trino.plugin.geospatial.SphericalGeographyType.SPHERICAL_GEOGRAPHY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.assertions.PlanMatchPattern.project;
import static io.trino.sql.planner.assertions.PlanMatchPattern.spatialLeftJoin;
import static io.trino.sql.planner.assertions.PlanMatchPattern.values;
import static io.trino.sql.planner.plan.JoinType.LEFT;
import static io.trino.sql.tree.ComparisonExpression.Operator.NOT_EQUAL;

public class TestExtractSpatialLeftJoin
        extends AbstractTestExtractSpatial
{
    @Test
    public void testDoesNotFire()
    {
        // scalar expression
        assertRuleApplication()
                .on(p ->
                {
                    Symbol b = p.symbol("b", GEOMETRY);
                    return p.join(LEFT,
                            p.values(),
                            p.values(b),
                            containsCall(geometryFromTextCall("POLYGON ..."), b.toSymbolReference()));
                })
                .doesNotFire();

        // OR operand
        assertRuleApplication()
                .on(p ->
                {
                    Symbol wkt = p.symbol("wkt", VARCHAR);
                    Symbol point = p.symbol("point", GEOMETRY);
                    Symbol name1 = p.symbol("name_1");
                    Symbol name2 = p.symbol("name_2");
                    return p.join(LEFT,
                            p.values(wkt, name1),
                            p.values(point, name2),
                            LogicalExpression.or(
                                    containsCall(geometryFromTextCall(wkt), point.toSymbolReference()),
                                    new ComparisonExpression(NOT_EQUAL, name1.toSymbolReference(), name2.toSymbolReference())));
                })
                .doesNotFire();

        // NOT operator
        assertRuleApplication()
                .on(p ->
                {
                    Symbol wkt = p.symbol("wkt", VARCHAR);
                    Symbol point = p.symbol("point", GEOMETRY);
                    Symbol name1 = p.symbol("name_1");
                    Symbol name2 = p.symbol("name_2");
                    return p.join(LEFT,
                            p.values(wkt, name1),
                            p.values(point, name2),
                            new NotExpression(containsCall(geometryFromTextCall(wkt), point.toSymbolReference())));
                })
                .doesNotFire();

        // ST_Distance(...) > r
        assertRuleApplication()
                .on(p ->
                {
                    Symbol a = p.symbol("a", GEOMETRY);
                    Symbol b = p.symbol("b", GEOMETRY);
                    return p.join(LEFT,
                            p.values(a),
                            p.values(b),
                            new ComparisonExpression(ComparisonExpression.Operator.GREATER_THAN,
                                    distanceCall(a.toSymbolReference(), b.toSymbolReference()),
                                    new LongLiteral("5")));
                })
                .doesNotFire();

        // SphericalGeography operand
        assertRuleApplication()
                .on(p ->
                {
                    Symbol a = p.symbol("a", SPHERICAL_GEOGRAPHY);
                    Symbol b = p.symbol("b", SPHERICAL_GEOGRAPHY);
                    return p.join(LEFT,
                            p.values(a),
                            p.values(b),
                            new ComparisonExpression(ComparisonExpression.Operator.GREATER_THAN,
                                    sphericalDistanceCall(a.toSymbolReference(), b.toSymbolReference()),
                                    new LongLiteral("5")));
                })
                .doesNotFire();

        // to_spherical_geography() operand
        assertRuleApplication()
                .on(p ->
                {
                    Symbol wkt = p.symbol("wkt", VARCHAR);
                    Symbol point = p.symbol("point", SPHERICAL_GEOGRAPHY);
                    return p.join(LEFT,
                            p.values(wkt),
                            p.values(point),
                            new ComparisonExpression(ComparisonExpression.Operator.GREATER_THAN,
                                    sphericalDistanceCall(toSphericalGeographyCall(wkt), point.toSymbolReference()),
                                    new LongLiteral("5")));
                })
                .doesNotFire();
    }

    @Test
    public void testConvertToSpatialJoin()
    {
        // symbols
        assertRuleApplication()
                .on(p ->
                {
                    Symbol a = p.symbol("a", GEOMETRY);
                    Symbol b = p.symbol("b", GEOMETRY);
                    return p.join(LEFT,
                            p.values(a),
                            p.values(b),
                            containsCall(a.toSymbolReference(), b.toSymbolReference()));
                })
                .matches(
                        spatialLeftJoin("ST_Contains(a, b)",
                                values(ImmutableMap.of("a", 0)),
                                values(ImmutableMap.of("b", 0))));

        // AND
        assertRuleApplication()
                .on(p ->
                {
                    Symbol a = p.symbol("a", GEOMETRY);
                    Symbol b = p.symbol("b", GEOMETRY);
                    Symbol name1 = p.symbol("name_1");
                    Symbol name2 = p.symbol("name_2");
                    return p.join(LEFT,
                            p.values(a, name1),
                            p.values(b, name2),
                            LogicalExpression.and(
                                    new ComparisonExpression(NOT_EQUAL, name1.toSymbolReference(), name2.toSymbolReference()),
                                    containsCall(a.toSymbolReference(), b.toSymbolReference())));
                })
                .matches(
                        spatialLeftJoin("name_1 != name_2 AND ST_Contains(a, b)",
                                values(ImmutableMap.of("a", 0, "name_1", 1)),
                                values(ImmutableMap.of("b", 0, "name_2", 1))));

        // AND
        assertRuleApplication()
                .on(p ->
                {
                    Symbol a1 = p.symbol("a1");
                    Symbol a2 = p.symbol("a2");
                    Symbol b1 = p.symbol("b1");
                    Symbol b2 = p.symbol("b2");
                    return p.join(LEFT,
                            p.values(a1, a2),
                            p.values(b1, b2),
                            LogicalExpression.and(
                                    containsCall(a1.toSymbolReference(), b1.toSymbolReference()),
                                    containsCall(a2.toSymbolReference(), b2.toSymbolReference())));
                })
                .matches(
                        spatialLeftJoin("ST_Contains(a1, b1) AND ST_Contains(a2, b2)",
                                values(ImmutableMap.of("a1", 0, "a2", 1)),
                                values(ImmutableMap.of("b1", 0, "b2", 1))));
    }

    @Test
    public void testPushDownFirstArgument()
    {
        assertRuleApplication()
                .on(p ->
                {
                    Symbol wkt = p.symbol("wkt", VARCHAR);
                    Symbol point = p.symbol("point", GEOMETRY);
                    return p.join(LEFT,
                            p.values(wkt),
                            p.values(point),
                            containsCall(geometryFromTextCall(wkt), point.toSymbolReference()));
                })
                .matches(
                        spatialLeftJoin("ST_Contains(st_geometryfromtext, point)",
                                project(ImmutableMap.of("st_geometryfromtext", PlanMatchPattern.expression("ST_GeometryFromText(wkt)")), values(ImmutableMap.of("wkt", 0))),
                                values(ImmutableMap.of("point", 0))));

        assertRuleApplication()
                .on(p ->
                {
                    Symbol wkt = p.symbol("wkt", VARCHAR);
                    return p.join(LEFT,
                            p.values(wkt),
                            p.values(),
                            containsCall(geometryFromTextCall(wkt), toPointCall(new LongLiteral("0"), new LongLiteral("0"))));
                })
                .doesNotFire();
    }

    @Test
    public void testPushDownSecondArgument()
    {
        assertRuleApplication()
                .on(p ->
                {
                    Symbol polygon = p.symbol("polygon", GEOMETRY);
                    Symbol lat = p.symbol("lat");
                    Symbol lng = p.symbol("lng");
                    return p.join(LEFT,
                            p.values(polygon),
                            p.values(lat, lng),
                            containsCall(polygon.toSymbolReference(), toPointCall(lng.toSymbolReference(), lat.toSymbolReference())));
                })
                .matches(
                        spatialLeftJoin("ST_Contains(polygon, st_point)",
                                values(ImmutableMap.of("polygon", 0)),
                                project(ImmutableMap.of("st_point", PlanMatchPattern.expression("ST_Point(lng, lat)")), values(ImmutableMap.of("lat", 0, "lng", 1)))));

        assertRuleApplication()
                .on(p ->
                {
                    Symbol lat = p.symbol("lat");
                    Symbol lng = p.symbol("lng");
                    return p.join(LEFT,
                            p.values(),
                            p.values(lat, lng),
                            containsCall(geometryFromTextCall("POLYGON ..."), toPointCall(lng.toSymbolReference(), lat.toSymbolReference())));
                })
                .doesNotFire();
    }

    @Test
    public void testPushDownBothArguments()
    {
        assertRuleApplication()
                .on(p ->
                {
                    Symbol wkt = p.symbol("wkt", VARCHAR);
                    Symbol lat = p.symbol("lat");
                    Symbol lng = p.symbol("lng");
                    return p.join(LEFT,
                            p.values(wkt),
                            p.values(lat, lng),
                            containsCall(geometryFromTextCall(wkt), toPointCall(lng.toSymbolReference(), lat.toSymbolReference())));
                })
                .matches(
                        spatialLeftJoin("ST_Contains(st_geometryfromtext, st_point)",
                                project(ImmutableMap.of("st_geometryfromtext", PlanMatchPattern.expression("ST_GeometryFromText(wkt)")), values(ImmutableMap.of("wkt", 0))),
                                project(ImmutableMap.of("st_point", PlanMatchPattern.expression("ST_Point(lng, lat)")), values(ImmutableMap.of("lat", 0, "lng", 1)))));
    }

    @Test
    public void testPushDownOppositeOrder()
    {
        assertRuleApplication()
                .on(p ->
                {
                    Symbol lat = p.symbol("lat");
                    Symbol lng = p.symbol("lng");
                    Symbol wkt = p.symbol("wkt", VARCHAR);
                    return p.join(LEFT,
                            p.values(lat, lng),
                            p.values(wkt),
                            containsCall(geometryFromTextCall(wkt), toPointCall(lng.toSymbolReference(), lat.toSymbolReference())));
                })
                .matches(
                        spatialLeftJoin("ST_Contains(st_geometryfromtext, st_point)",
                                project(ImmutableMap.of("st_point", PlanMatchPattern.expression("ST_Point(lng, lat)")), values(ImmutableMap.of("lat", 0, "lng", 1))),
                                project(ImmutableMap.of("st_geometryfromtext", PlanMatchPattern.expression("ST_GeometryFromText(wkt)")), values(ImmutableMap.of("wkt", 0)))));
    }

    @Test
    public void testPushDownAnd()
    {
        assertRuleApplication()
                .on(p ->
                {
                    Symbol wkt = p.symbol("wkt", VARCHAR);
                    Symbol lat = p.symbol("lat");
                    Symbol lng = p.symbol("lng");
                    Symbol name1 = p.symbol("name_1");
                    Symbol name2 = p.symbol("name_2");
                    return p.join(LEFT,
                            p.values(wkt, name1),
                            p.values(lat, lng, name2),
                            LogicalExpression.and(
                                    new ComparisonExpression(NOT_EQUAL, name1.toSymbolReference(), name2.toSymbolReference()),
                                    containsCall(geometryFromTextCall(wkt), toPointCall(lng.toSymbolReference(), lat.toSymbolReference()))));
                })
                .matches(
                        spatialLeftJoin("name_1 != name_2 AND ST_Contains(st_geometryfromtext, st_point)",
                                project(ImmutableMap.of("st_geometryfromtext", PlanMatchPattern.expression("ST_GeometryFromText(wkt)")), values(ImmutableMap.of("wkt", 0, "name_1", 1))),
                                project(ImmutableMap.of("st_point", PlanMatchPattern.expression("ST_Point(lng, lat)")), values(ImmutableMap.of("lat", 0, "lng", 1, "name_2", 2)))));

        // Multiple spatial functions - only the first one is being processed
        assertRuleApplication()
                .on(p ->
                {
                    Symbol wkt1 = p.symbol("wkt1", VARCHAR);
                    Symbol wkt2 = p.symbol("wkt2", VARCHAR);
                    Symbol geometry1 = p.symbol("geometry1");
                    Symbol geometry2 = p.symbol("geometry2");
                    return p.join(LEFT,
                            p.values(wkt1, wkt2),
                            p.values(geometry1, geometry2),
                            LogicalExpression.and(
                                    containsCall(geometryFromTextCall(wkt1), geometry1.toSymbolReference()),
                                    containsCall(geometryFromTextCall(wkt2), geometry2.toSymbolReference())));
                })
                .matches(
                        spatialLeftJoin("ST_Contains(st_geometryfromtext, geometry1) AND ST_Contains(ST_GeometryFromText(wkt2), geometry2)",
                                project(ImmutableMap.of("st_geometryfromtext", PlanMatchPattern.expression("ST_GeometryFromText(wkt1)")), values(ImmutableMap.of("wkt1", 0, "wkt2", 1))),
                                values(ImmutableMap.of("geometry1", 0, "geometry2", 1))));
    }

    private RuleBuilder assertRuleApplication()
    {
        RuleTester tester = tester();
        return tester.assertThat(new ExtractSpatialJoins.ExtractSpatialLeftJoin(tester.getPlannerContext(), tester.getSplitManager(), tester.getPageSourceManager(), tester.getTypeAnalyzer()));
    }
}
