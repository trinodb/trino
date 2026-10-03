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

package io.trino.spi.eventlistener;

/**
 * How an output column's value is derived from its source columns.
 * <p>
 * These correspond to the subtypes of a {@code DIRECT} transformation in the
 * <a href="https://openlineage.io/docs/spec/facets/dataset-facets/column_lineage_facet">OpenLineage column lineage facet</a>.
 * Trino only tracks {@code DIRECT} lineage (a source column contributes to an output column); it does not track
 * {@code INDIRECT} transformations (join, filter, group-by, sort), so those subtypes are not represented here.
 * <p>
 * Classification is per source-column→output-column edge and conservative: an edge is reported as {@link #AGGREGATION}
 * only when no raw value of that source column survives into the output by any path. Subtypes propagate through
 * subqueries, common table expressions, views and set operations. When Trino cannot determine an edge's derivation,
 * no subtype is reported.
 */
public enum ColumnTransformationType
{
    /**
     * The output column is a straight copy of a source column, for example {@code SELECT a}.
     */
    IDENTITY(0),
    /**
     * The output column is derived from its source columns via a non-aggregate expression, for example
     * {@code SELECT a + b} or {@code SELECT concat(a, b)}.
     */
    TRANSFORMATION(1),
    /**
     * Every source column reaches the output column only through an aggregate function, for example
     * {@code SELECT sum(a)} or {@code SELECT count(*)}. Note that some aggregate functions such as
     * {@code min}, {@code max} and {@code first_value} still return an unmodified source value.
     */
    AGGREGATION(2);

    private final int derivationRank;

    ColumnTransformationType(int derivationRank)
    {
        this.derivationRank = derivationRank;
    }

    public int getDerivationRank()
    {
        return derivationRank;
    }
}
