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
package io.trino.plugin.bigquery;

import com.google.cloud.bigquery.Field;
import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.Type;

import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;

public enum BigQueryPseudoColumn
{
    PARTITION_DATE("$partition_date", "_PARTITIONDATE", DATE, BigQueryType.DATE),
    PARTITION_TIME("$partition_time", "_PARTITIONTIME", TIMESTAMP_TZ_MICROS, BigQueryType.TIMESTAMP),
    /**/;

    private final String trinoColumnName;
    private final String bigqueryColumnName;
    private final Type trinoType;
    private final BigQueryType bigqueryType;

    BigQueryPseudoColumn(String trinoColumnName, String bigqueryColumnName, Type type, BigQueryType bigqueryType)
    {
        this.trinoColumnName = trinoColumnName;
        this.bigqueryColumnName = bigqueryColumnName;
        this.trinoType = type;
        this.bigqueryType = bigqueryType;
    }

    public String getTrinoColumnName()
    {
        return trinoColumnName;
    }

    public String getBigqueryColumnName()
    {
        return bigqueryColumnName;
    }

    public BigQueryColumnHandle getColumnHandle()
    {
        return new BigQueryColumnHandle(
                trinoColumnName,
                bigqueryType,
                Field.Mode.REQUIRED,
                null,
                null,
                ImmutableList.of(),
                null,
                true);
    }

    public ColumnMetadata getColumnMetadata()
    {
        return ColumnMetadata.builder()
                .setName(trinoColumnName)
                .setType(trinoType)
                .setHidden(true)
                .build();
    }
}
