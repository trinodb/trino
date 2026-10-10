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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import io.airlift.http.client.HeaderName;
import io.airlift.http.client.Request;
import io.airlift.http.client.testing.TestingHttpClient;
import io.airlift.json.JsonCodec;
import io.trino.plugin.pinot.auth.PinotBrokerAuthenticationProvider;
import io.trino.plugin.pinot.auth.PinotControllerAuthenticationProvider;
import io.trino.plugin.pinot.auth.none.PinotEmptyAuthenticationProvider;
import io.trino.plugin.pinot.client.IdentityPinotHostMapper;
import io.trino.plugin.pinot.client.PinotClient;
import org.apache.pinot.spi.data.Schema;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.airlift.concurrent.Threads.threadsNamed;
import static io.trino.plugin.pinot.MetadataUtil.BROKERS_FOR_TABLE_JSON_CODEC;
import static io.trino.plugin.pinot.MetadataUtil.BROKER_RESPONSE_NATIVE_JSON_CODEC;
import static io.trino.plugin.pinot.MetadataUtil.TABLES_JSON_CODEC;
import static io.trino.plugin.pinot.MetadataUtil.TEST_TABLE;
import static io.trino.plugin.pinot.MetadataUtil.TIME_BOUNDARY_JSON_CODEC;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.stream.Collectors.toList;

public class MockPinotClient
        extends PinotClient
{
    private final String response;
    private final Map<String, Schema> metadata;

    public MockPinotClient(PinotConfig pinotConfig)
    {
        this(pinotConfig, ImmutableMap.of(), null);
    }

    public MockPinotClient(PinotConfig pinotConfig, Map<String, Schema> metadata)
    {
        this(pinotConfig, metadata, null);
    }

    public MockPinotClient(PinotConfig pinotConfig, Map<String, Schema> metadata, String response)
    {
        super(pinotConfig,
                new IdentityPinotHostMapper(),
                new TestingHttpClient(_ -> null),
                newCachedThreadPool(threadsNamed("pinot-metadata-fetcher-testing")),
                TABLES_JSON_CODEC,
                BROKERS_FOR_TABLE_JSON_CODEC,
                TIME_BOUNDARY_JSON_CODEC,
                BROKER_RESPONSE_NATIVE_JSON_CODEC,
                PinotControllerAuthenticationProvider.create(PinotEmptyAuthenticationProvider.instance()),
                PinotBrokerAuthenticationProvider.create(PinotEmptyAuthenticationProvider.instance()));
        this.metadata = metadata;
        this.response = response;
    }

    @Override
    public String getBrokerHost(String table)
    {
        return "localhost";
    }

    @Override
    public <T> T doHttpActionWithHeadersJson(
            Request.Builder requestBuilder,
            Optional<String> requestBody,
            JsonCodec<T> codec,
            Multimap<HeaderName, String> additionalHeaders)
    {
        return codec.fromJson(response);
    }

    @Override
    public Multimap<String, String> getAllTables()
    {
        return ImmutableListMultimap.<String, String>builder()
                .put(TestPinotSplitManager.realtimeOnlyTable.tableName().toLowerCase(ENGLISH), TestPinotSplitManager.realtimeOnlyTable.tableName())
                .put(TestPinotSplitManager.hybridTable.tableName().toLowerCase(ENGLISH), TestPinotSplitManager.hybridTable.tableName())
                .put(TEST_TABLE.toLowerCase(ENGLISH), TEST_TABLE)
                .putAll(metadata.keySet().stream()
                        .map(key -> new AbstractMap.SimpleEntry<>(key.toLowerCase(ENGLISH), key))
                        .collect(toList()))
                .build();
    }

    @Override
    public Map<String, Map<String, List<String>>> getRoutingTableForTable(String tableName)
    {
        ImmutableMap.Builder<String, Map<String, List<String>>> routingTable = ImmutableMap.builder();

        if (TestPinotSplitManager.realtimeOnlyTable.tableName().equalsIgnoreCase(tableName) || TestPinotSplitManager.hybridTable.tableName().equalsIgnoreCase(tableName)) {
            routingTable.put(tableName + "_REALTIME", ImmutableMap.of(
                    "server1", ImmutableList.of("segment11", "segment12"),
                    "server2", ImmutableList.of("segment21", "segment22")));
        }

        if (TestPinotSplitManager.hybridTable.tableName().equalsIgnoreCase(tableName)) {
            routingTable.put(tableName + "_OFFLINE", ImmutableMap.of(
                    "server3", ImmutableList.of("segment31", "segment32"),
                    "server4", ImmutableList.of("segment41", "segment42")));
        }

        return routingTable.buildOrThrow();
    }

    @Override
    public Schema getTableSchema(String table)
            throws Exception
    {
        Schema schema = metadata.get(table);
        if (schema != null) {
            return schema;
        }
        // From the test pinot table airlineStats
        return Schema.fromString(
                """
                {
                  "schemaName": "airlineStats",
                  "dimensionFieldSpecs": [
                    {
                      "name": "ActualElapsedTime",
                      "dataType": "INT"
                    },
                    {
                      "name": "AirTime",
                      "dataType": "INT"
                    },
                    {
                      "name": "AirlineID",
                      "dataType": "INT"
                    },
                    {
                      "name": "ArrDel15",
                      "dataType": "INT"
                    },
                    {
                      "name": "ArrDelay",
                      "dataType": "INT"
                    },
                    {
                      "name": "ArrDelayMinutes",
                      "dataType": "INT"
                    },
                    {
                      "name": "ArrTime",
                      "dataType": "INT"
                    },
                    {
                      "name": "ArrTimeBlk",
                      "dataType": "STRING"
                    },
                    {
                      "name": "ArrivalDelayGroups",
                      "dataType": "INT"
                    },
                    {
                      "name": "CRSArrTime",
                      "dataType": "INT"
                    },
                    {
                      "name": "CRSDepTime",
                      "dataType": "INT"
                    },
                    {
                      "name": "CRSElapsedTime",
                      "dataType": "INT"
                    },
                    {
                      "name": "CancellationCode",
                      "dataType": "STRING"
                    },
                    {
                      "name": "Cancelled",
                      "dataType": "INT"
                    },
                    {
                      "name": "Carrier",
                      "dataType": "STRING"
                    },
                    {
                      "name": "CarrierDelay",
                      "dataType": "INT"
                    },
                    {
                      "name": "DayOfWeek",
                      "dataType": "INT"
                    },
                    {
                      "name": "DayofMonth",
                      "dataType": "INT"
                    },
                    {
                      "name": "DepDel15",
                      "dataType": "INT"
                    },
                    {
                      "name": "DepDelay",
                      "dataType": "INT"
                    },
                    {
                      "name": "DepDelayMinutes",
                      "dataType": "INT"
                    },
                    {
                      "name": "DepTime",
                      "dataType": "INT"
                    },
                    {
                      "name": "DepTimeBlk",
                      "dataType": "STRING"
                    },
                    {
                      "name": "DepartureDelayGroups",
                      "dataType": "INT"
                    },
                    {
                      "name": "Dest",
                      "dataType": "STRING"
                    },
                    {
                      "name": "DestAirportID",
                      "dataType": "INT"
                    },
                    {
                      "name": "DestAirportSeqID",
                      "dataType": "INT"
                    },
                    {
                      "name": "DestCityMarketID",
                      "dataType": "INT"
                    },
                    {
                      "name": "DestCityName",
                      "dataType": "STRING"
                    },
                    {
                      "name": "DestState",
                      "dataType": "STRING"
                    },
                    {
                      "name": "DestStateFips",
                      "dataType": "INT"
                    },
                    {
                      "name": "DestStateName",
                      "dataType": "STRING"
                    },
                    {
                      "name": "DestWac",
                      "dataType": "INT"
                    },
                    {
                      "name": "Distance",
                      "dataType": "INT"
                    },
                    {
                      "name": "DistanceGroup",
                      "dataType": "INT"
                    },
                    {
                      "name": "DivActualElapsedTime",
                      "dataType": "INT"
                    },
                    {
                      "name": "DivAirportIDs",
                      "dataType": "INT",
                      "singleValueField": false
                    },
                    {
                      "name": "DivAirportLandings",
                      "dataType": "INT"
                    },
                    {
                      "name": "DivAirportSeqIDs",
                      "dataType": "INT",
                      "singleValueField": false
                    },
                    {
                      "name": "DivAirports",
                      "dataType": "STRING",
                      "singleValueField": false
                    },
                    {
                      "name": "DivArrDelay",
                      "dataType": "INT"
                    },
                    {
                      "name": "DivDistance",
                      "dataType": "INT"
                    },
                    {
                      "name": "DivLongestGTimes",
                      "dataType": "INT",
                      "singleValueField": false
                    },
                    {
                      "name": "DivReachedDest",
                      "dataType": "INT"
                    },
                    {
                      "name": "DivTailNums",
                      "dataType": "STRING",
                      "singleValueField": false
                    },
                    {
                      "name": "DivTotalGTimes",
                      "dataType": "INT",
                      "singleValueField": false
                    },
                    {
                      "name": "DivWheelsOffs",
                      "dataType": "INT",
                      "singleValueField": false
                    },
                    {
                      "name": "DivWheelsOns",
                      "dataType": "INT",
                      "singleValueField": false
                    },
                    {
                      "name": "Diverted",
                      "dataType": "INT"
                    },
                    {
                      "name": "FirstDepTime",
                      "dataType": "INT"
                    },
                    {
                      "name": "FlightDate",
                      "dataType": "STRING"
                    },
                    {
                      "name": "FlightNum",
                      "dataType": "INT"
                    },
                    {
                      "name": "Flights",
                      "dataType": "INT"
                    },
                    {
                      "name": "LateAircraftDelay",
                      "dataType": "INT"
                    },
                    {
                      "name": "LongestAddGTime",
                      "dataType": "INT"
                    },
                    {
                      "name": "Month",
                      "dataType": "INT"
                    },
                    {
                      "name": "NASDelay",
                      "dataType": "INT"
                    },
                    {
                      "name": "Origin",
                      "dataType": "STRING"
                    },
                    {
                      "name": "OriginAirportID",
                      "dataType": "INT"
                    },
                    {
                      "name": "OriginAirportSeqID",
                      "dataType": "INT"
                    },
                    {
                      "name": "OriginCityMarketID",
                      "dataType": "INT"
                    },
                    {
                      "name": "OriginCityName",
                      "dataType": "STRING"
                    },
                    {
                      "name": "OriginState",
                      "dataType": "STRING"
                    },
                    {
                      "name": "OriginStateFips",
                      "dataType": "INT"
                    },
                    {
                      "name": "OriginStateName",
                      "dataType": "STRING"
                    },
                    {
                      "name": "OriginWac",
                      "dataType": "INT"
                    },
                    {
                      "name": "Quarter",
                      "dataType": "INT"
                    },
                    {
                      "name": "RandomAirports",
                      "dataType": "STRING",
                      "singleValueField": false
                    },
                    {
                      "name": "SecurityDelay",
                      "dataType": "INT"
                    },
                    {
                      "name": "TailNum",
                      "dataType": "STRING"
                    },
                    {
                      "name": "TaxiIn",
                      "dataType": "INT"
                    },
                    {
                      "name": "TaxiOut",
                      "dataType": "INT"
                    },
                    {
                      "name": "Year",
                      "dataType": "INT"
                    },
                    {
                      "name": "WheelsOn",
                      "dataType": "INT"
                    },
                    {
                      "name": "WheelsOff",
                      "dataType": "INT"
                    },
                    {
                      "name": "WeatherDelay",
                      "dataType": "INT"
                    },
                    {
                      "name": "UniqueCarrier",
                      "dataType": "STRING"
                    },
                    {
                      "name": "TotalAddGTime",
                      "dataType": "INT"
                    }
                  ],
                  "timeFieldSpec": {
                    "incomingGranularitySpec": {
                      "name": "DaysSinceEpoch",
                      "dataType": "INT",
                      "timeType": "DAYS"
                    }
                  },
                  "updateSemantic": null
                }
                """);
    }

    @Override
    public TimeBoundary getTimeBoundaryForTable(String table)
    {
        if (TestPinotSplitManager.hybridTable.tableName().equalsIgnoreCase(table)) {
            return new TimeBoundary("secondsSinceEpoch", "4562345");
        }

        return new TimeBoundary();
    }
}
