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
package io.trino.plugin.pinot;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.type.Type;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DateTimeFormatSpec;

import java.util.concurrent.TimeUnit;
import java.util.function.LongUnaryOperator;

import static io.trino.plugin.pinot.PinotColumn.getTrinoTypeFromPinotType;
import static java.util.Objects.requireNonNull;

public class PinotDateTimeField
{
    private static final String DATE_TIME_FORMAT = "EPOCH";
    private final String columnName;
    private final TimeUnit timeUnit;
    private final Type type;

    public PinotDateTimeField(DateTimeFieldSpec dateTimeFieldSpec)
    {
        this(dateTimeFieldSpec.getName(), new DateTimeFormatSpec(dateTimeFieldSpec.getFormat()).getColumnUnit(), getTrinoTypeFromPinotType(dateTimeFieldSpec.getDataType()));
    }

    @JsonCreator
    public PinotDateTimeField(
            @JsonProperty String columnName,
            @JsonProperty TimeUnit timeUnit,
            @JsonProperty Type type)
    {
        this.columnName = requireNonNull(columnName, "columnName is null");
        this.timeUnit = requireNonNull(timeUnit, "timeUnit is null");
        this.type = requireNonNull(type, "type is null");
    }

    @JsonProperty
    public String getColumnName()
    {
        return columnName;
    }

    @JsonProperty
    public TimeUnit getTimeUnit()
    {
        return timeUnit;
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }

    public LongUnaryOperator getToMillisTransform()
    {
        return incomingValue -> timeUnit.toMillis(incomingValue);
    }
}
