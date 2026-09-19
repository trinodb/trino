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
package io.trino.plugin.oracle;

import com.google.inject.Inject;
import io.trino.plugin.jdbc.DefaultQueryBuilder;
import io.trino.plugin.jdbc.JdbcClient;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.QueryParameter;
import io.trino.plugin.jdbc.expression.ParameterizedExpression;
import io.trino.plugin.jdbc.logging.RemoteQueryModifier;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static java.lang.String.format;

/**
 * Wraps Oracle Spatial columns in the SELECT projection so their values can be materialized
 * through JDBC without needing the {@code oracle.xdb.XMLType} class. The Oracle JDBC driver's
 * default OPAQUE handler tries to load {@code oracle.xdb.XMLType} to represent SDO_GEOMETRY /
 * SDO_POINT_TYPE values; the {@code xdb} module is not on the Trino classpath, so any read of
 * a raw SDO column fails with {@code NoClassDefFoundError: oracle/xdb/XMLType}. Converting the
 * value to text server-side via {@code MDSYS.SDO_UTIL.TO_WKTGEOMETRY} sidesteps that path
 * entirely — the driver just sees a CLOB.
 */
public class OracleQueryBuilder
        extends DefaultQueryBuilder
{
    @Inject
    public OracleQueryBuilder(RemoteQueryModifier queryModifier)
    {
        super(queryModifier);
    }

    @Override
    protected String getProjection(JdbcClient client, List<JdbcColumnHandle> columns, Map<String, ParameterizedExpression> columnExpressions, Consumer<QueryParameter> accumulator)
    {
        if (columns.isEmpty()) {
            return "1 x";
        }
        List<String> projections = new ArrayList<>();
        for (JdbcColumnHandle jdbcColumnHandle : columns) {
            String columnAlias = client.quoted(jdbcColumnHandle.getColumnName());
            ParameterizedExpression expression = columnExpressions.get(jdbcColumnHandle.getColumnName());
            if (expression != null) {
                projections.add(format("%s AS %s", expression.expression(), columnAlias));
                expression.parameters().forEach(accumulator);
                continue;
            }
            String remoteTypeName = jdbcColumnHandle.getJdbcTypeHandle().jdbcTypeName().orElse("");
            String wrapped = wrapSpatialColumn(columnAlias, remoteTypeName);
            if (wrapped != null) {
                projections.add(format("%s AS %s", wrapped, columnAlias));
            }
            else {
                projections.add(columnAlias);
            }
        }
        return String.join(", ", projections);
    }

    /**
     * Returns a server-side expression that converts the raw SDO column to a WKT string,
     * or {@code null} if the column is not a spatial UDT and needs no wrapping.
     */
    private static String wrapSpatialColumn(String quotedColumn, String remoteTypeName)
    {
        if (remoteTypeName.equalsIgnoreCase("SDO_GEOMETRY")) {
            return "MDSYS.SDO_UTIL.TO_WKTGEOMETRY(" + quotedColumn + ")";
        }
        if (remoteTypeName.equalsIgnoreCase("SDO_POINT_TYPE")) {
            // SDO_UTIL only operates on SDO_GEOMETRY, so promote the point to a 2001 (2-D point)
            // geometry first. Guard the NULL case explicitly — the SDO_GEOMETRY constructor
            // materializes a non-NULL geometry even from a NULL point, and we want NULL in, NULL out.
            return "CASE WHEN " + quotedColumn + " IS NULL THEN NULL "
                    + "ELSE MDSYS.SDO_UTIL.TO_WKTGEOMETRY(MDSYS.SDO_GEOMETRY(2001, NULL, " + quotedColumn + ", NULL, NULL)) "
                    + "END";
        }
        return null;
    }
}
