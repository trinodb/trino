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
package io.trino.plugin.elasticsearch.decoders;

import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.Type;

import static java.lang.String.format;

public enum DecoderType
{
    ARRAY,
    BIGINT,
    BOOLEAN,
    DOUBLE,
    INTEGER,
    IP_ADDRESS,
    RAW_JSON,
    REAL,
    ROW,
    SMALLINT,
    TIMESTAMP,
    TIMESTAMP_WITH_TIME_ZONE,
    TINYINT,
    VARBINARY,
    VARCHAR/**/;

    public static DecoderType of(Type type)
    {
        switch (type.getBaseName()) {
            case StandardTypes.ARRAY:
                return ARRAY;
            case StandardTypes.BIGINT:
                return BIGINT;
            case StandardTypes.BOOLEAN:
                return BOOLEAN;
            case StandardTypes.DOUBLE:
                return DOUBLE;
            case StandardTypes.INTEGER:
                return INTEGER;
            case StandardTypes.IPADDRESS:
                return IP_ADDRESS;
            case StandardTypes.REAL:
                return REAL;
            case StandardTypes.ROW:
                return ROW;
            case StandardTypes.SMALLINT:
                return SMALLINT;
            case StandardTypes.TIMESTAMP:
                return TIMESTAMP;
            case StandardTypes.TIMESTAMP_WITH_TIME_ZONE:
                throw new IllegalStateException("TIMESTAMP_WITH_TIME_ZONE is not a supported type to decode.");
            case StandardTypes.TINYINT:
                return TINYINT;
            case StandardTypes.VARBINARY:
                return VARBINARY;
            case StandardTypes.VARCHAR:
                return VARCHAR;
        }
        throw new IllegalStateException(format("no decoder types for type: %s", type.getBaseName()));
    }
}
