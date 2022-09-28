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
package io.trino.plugin.kudu.procedures;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.kudu.KuduClientSession;
import io.trino.plugin.kudu.properties.KuduTableProperties;
import io.trino.plugin.kudu.properties.RangePartition;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.procedure.Procedure.Argument;

import javax.inject.Inject;

import java.lang.invoke.MethodHandle;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.invoke.MethodHandles.lookup;
import static java.util.Objects.requireNonNull;

public class RangePartitionProcedures
{
    private static final MethodHandle ADD;
    private static final MethodHandle DROP;

    static {
        try {
            ADD = lookup().unreflect(RangePartitionProcedures.class.getMethod("addRangePartition", String.class, String.class, String.class));
            DROP = lookup().unreflect(RangePartitionProcedures.class.getMethod("dropRangePartition", String.class, String.class, String.class));
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private final KuduClientSession clientSession;

    @Inject
    public RangePartitionProcedures(KuduClientSession clientSession)
    {
        this.clientSession = requireNonNull(clientSession);
    }

    public Procedure getAddPartitionProcedure()
    {
        return new Procedure(
                "system",
                "add_range_partition",
                ImmutableList.of(
                        new Argument("SCHEMA", VARCHAR),
                        new Argument("TABLE", VARCHAR),
                        new Argument("RANGE_BOUNDS", VARCHAR)),
                ADD.bindTo(this));
    }

    public Procedure getDropPartitionProcedure()
    {
        return new Procedure(
                "system",
                "drop_range_partition",
                ImmutableList.of(
                        new Argument("SCHEMA", VARCHAR),
                        new Argument("TABLE", VARCHAR),
                        new Argument("RANGE_BOUNDS", VARCHAR)),
                DROP.bindTo(this));
    }

    public void addRangePartition(String schema, String table, String rangeBounds)
    {
        SchemaTableName schemaTableName = new SchemaTableName(schema, table);
        RangePartition rangePartition = KuduTableProperties.parseRangePartition(rangeBounds);
        clientSession.addRangePartition(schemaTableName, rangePartition);
    }

    public void dropRangePartition(String schema, String table, String rangeBounds)
    {
        SchemaTableName schemaTableName = new SchemaTableName(schema, table);
        RangePartition rangePartition = KuduTableProperties.parseRangePartition(rangeBounds);
        clientSession.dropRangePartition(schemaTableName, rangePartition);
    }
}
