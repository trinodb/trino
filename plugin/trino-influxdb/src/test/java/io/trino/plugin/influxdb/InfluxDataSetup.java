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
package io.trino.plugin.influxdb;

import com.google.common.collect.Maps;
import io.trino.spi.type.Type;
import io.trino.testing.datatype.ColumnSetup;
import io.trino.testing.datatype.DataSetup;
import io.trino.testing.sql.TemporaryRelation;
import org.influxdb.dto.Point;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;

import static io.trino.plugin.influxdb.TypeUtils.toTrinoType;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static java.lang.Boolean.parseBoolean;
import static java.lang.Double.parseDouble;
import static java.lang.Long.parseLong;
import static java.lang.String.format;
import static java.time.format.DateTimeFormatter.ISO_DATE_TIME;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class InfluxDataSetup
        implements DataSetup
{
    private static final String TEST_DATABASE_TPCH = "tpch";
    private final InfluxSession session;
    private final String tableNamePrefix;

    public InfluxDataSetup(InfluxSession session, String tableNamePrefix)
    {
        this.session = requireNonNull(session, "session is null");
        this.tableNamePrefix = requireNonNull(tableNamePrefix, "tableNamePrefix is null");
    }

    @Override
    public TemporaryRelation setupTemporaryRelation(List<ColumnSetup> inputs)
    {
        TestingInfluxTable testTable = new TestingInfluxTable(this.tableNamePrefix);
        try {
            insertRowsWithExpectedValues(inputs, testTable);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
        return testTable;
    }

    private void insertRowsWithExpectedValues(List<ColumnSetup> inputs, TestingInfluxTable testTable)
    {
        Map<String, Object> nullValueFields = Maps.newHashMapWithExpectedSize(inputs.size());
        Point.Builder builder = Point.measurement(testTable.getName());
        int index = 0;
        for (ColumnSetup input : inputs) {
            String filedName = format("col_%s", index);
            String inputType = input.getDeclaredType().orElse("string");
            String inputValue = input.getInputLiteral();
            if ("NULL".equalsIgnoreCase(inputValue)) {
                nullValueFields.put(filedName, null);
                continue;
            }
            if (index == 0) {
                Instant utc = LocalDateTime.parse(inputValue, ISO_DATE_TIME).atZone(ZoneId.of("UTC")).toInstant();
                long time = utc.getEpochSecond() * 1_000_000_000L + utc.getNano();
                builder.time(time, NANOSECONDS);
            }
            else {
                Type type = toTrinoType(inputType);
                if (type == BOOLEAN) {
                    boolean fieldValue = parseBoolean(inputValue);
                    builder.addField(filedName, fieldValue);
                }
                else if (type == BIGINT) {
                    long fieldValue = parseLong(inputValue);
                    builder.addField(filedName, fieldValue);
                }
                else if (type == DOUBLE) {
                    double fieldValue = parseDouble(inputValue);
                    builder.addField(filedName, fieldValue);
                }
                else {
                    builder.addField(filedName, inputValue);
                }
            }
            index++;
        }
        builder.fields(nullValueFields);
        session.write(TEST_DATABASE_TPCH, builder.build());
    }
}
