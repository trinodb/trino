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
import com.google.common.collect.ImmutableMap;
import io.airlift.http.client.Request;
import io.airlift.http.client.testing.TestingHttpClient;
import io.airlift.json.JsonCodec;
import io.trino.plugin.pinot.client.IdentityPinotHostMapper;
import io.trino.plugin.pinot.client.PinotClient;
import org.apache.pinot.spi.data.Schema;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.pinot.MetadataUtil.BROKERS_FOR_TABLE_JSON_CODEC;
import static io.trino.plugin.pinot.MetadataUtil.BROKER_RESPONSE_NATIVE_JSON_CODEC;
import static io.trino.plugin.pinot.MetadataUtil.TABLES_JSON_CODEC;
import static io.trino.plugin.pinot.MetadataUtil.TEST_TABLE;
import static io.trino.plugin.pinot.MetadataUtil.TIME_BOUNDARY_JSON_CODEC;

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
        super(
                pinotConfig,
                new IdentityPinotHostMapper(),
                new TestingHttpClient(request -> null),
                TABLES_JSON_CODEC,
                BROKERS_FOR_TABLE_JSON_CODEC,
                TIME_BOUNDARY_JSON_CODEC,
                BROKER_RESPONSE_NATIVE_JSON_CODEC);
        this.metadata = metadata;
        this.response = response;
    }

    @Override
    public String getBrokerHost(String table)
    {
        return "localhost";
    }

    @Override
    public <T> T doHttpActionWithHeadersJson(Request.Builder requestBuilder, Optional<String> requestBody, JsonCodec<T> codec)
    {
        return codec.fromJson(response);
    }

    @Override
    public List<String> getAllTables()
    {
        return ImmutableList.<String>builder()
                .add(TestPinotSplitManager.realtimeOnlyTable.getTableName())
                .add(TestPinotSplitManager.hybridTable.getTableName())
                .add(TEST_TABLE)
                .addAll(metadata.keySet())
                .build();
    }

    @Override
    public Map<String, Map<String, List<String>>> getRoutingTableForTable(String tableName)
    {
        ImmutableMap.Builder<String, Map<String, List<String>>> routingTable = ImmutableMap.builder();

        if (TestPinotSplitManager.realtimeOnlyTable.getTableName().equalsIgnoreCase(tableName) || TestPinotSplitManager.hybridTable.getTableName().equalsIgnoreCase(tableName)) {
            routingTable.put(tableName + "_REALTIME", ImmutableMap.of(
                    "server1", ImmutableList.of("segment11", "segment12"),
                    "server2", ImmutableList.of("segment21", "segment22")));
        }

        if (TestPinotSplitManager.hybridTable.getTableName().equalsIgnoreCase(tableName)) {
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
        return Schema.fromString("{\n" +
                "  \"schemaName\": \"airlineStats\",\n" +
                "  \"dimensionFieldSpecs\": [\n" +
                "    {\n" +
                "      \"name\": \"ActualElapsedTime\",\n" +
                "      \"dataType\": \"INT\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"AirTime\",\n" +
                "      \"dataType\": \"INT\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"AirlineID\",\n" +
                "      \"dataType\": \"INT\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"ArrDel15\",\n" +
                "      \"dataType\": \"INT\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"ArrDelay\",\n" +
                "      \"dataType\": \"INT\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"ArrDelayMinutes\",\n" +
                "      \"dataType\": \"INT\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"ArrTime\",\n" +
                "      \"dataType\": \"INT\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"ArrTimeBlk\",\n" +
                "      \"dataType\": \"STRING\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"ArrivalDelayGroups\",\n" +
                "      \"dataType\": \"INT\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"CRSArrTime\",\n" +
                "      \"dataType\": \"INT\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"CRSDepTime\",\n" +
                "      \"dataType\": \"INT\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"CRSElapsedTime\",\n" +
                "      \"dataType\": \"INT\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"CancellationCode\",\n" +
                "      \"dataType\": \"STRING\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"Cancelled\",\n" +
                "      \"dataType\": \"INT\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"Carrier\",\n" +
                "      \"dataType\": \"STRING\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"CarrierDelay\",\n" +
                "      \"dataType\": \"INT\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"DayOfWeek\",\n" +
                "      \"dataType\": \"INT\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"DayofMonth\",\n" +
                "      \"dataType\": \"INT\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"DepDel15\",\n" +
                "      \"dataType\": \"INT\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"DepDelay\",\n" +
                "      \"dataType\": \"INT\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"DepDelayMinutes\",\n" +
                "      \"dataType\": \"INT\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"DepTime\",\n" +
                "      \"dataType\": \"INT\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"DepTimeBlk\",\n" +
                "      \"dataType\": \"STRING\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"DepartureDelayGroups\",\n" +
                "      \"dataType\": \"INT\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"Dest\",\n" +
                "      \"dataType\": \"STRING\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"DestAirportID\",\n" +
                "      \"dataType\": \"INT\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"DestAirportSeqID\",\n" +
                "      \"dataType\": \"INT\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"DestCityMarketID\",\n" +
                "      \"dataType\": \"INT\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"DestCityName\",\n" +
                "      \"dataType\": \"STRING\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"DestState\",\n" +
                "      \"dataType\": \"STRING\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"DestStateFips\",\n" +
                "      \"dataType\": \"INT\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"DestStateName\",\n" +
                "      \"dataType\": \"STRING\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"DestWac\",\n" +
                "      \"dataType\": \"INT\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"Distance\",\n" +
                "      \"dataType\": \"INT\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"DistanceGroup\",\n" +
                "      \"dataType\": \"INT\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"DivActualElapsedTime\",\n" +
                "      \"dataType\": \"INT\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"DivAirportIDs\",\n" +
                "      \"dataType\": \"INT\",\n" +
                "      \"singleValueField\": false\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"DivAirportLandings\",\n" +
                "      \"dataType\": \"INT\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"DivAirportSeqIDs\",\n" +
                "      \"dataType\": \"INT\",\n" +
                "      \"singleValueField\": false\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"DivAirports\",\n" +
                "      \"dataType\": \"STRING\",\n" +
                "      \"singleValueField\": false\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"DivArrDelay\",\n" +
                "      \"dataType\": \"INT\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"DivDistance\",\n" +
                "      \"dataType\": \"INT\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"DivLongestGTimes\",\n" +
                "      \"dataType\": \"INT\",\n" +
                "      \"singleValueField\": false\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"DivReachedDest\",\n" +
                "      \"dataType\": \"INT\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"DivTailNums\",\n" +
                "      \"dataType\": \"STRING\",\n" +
                "      \"singleValueField\": false\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"DivTotalGTimes\",\n" +
                "      \"dataType\": \"INT\",\n" +
                "      \"singleValueField\": false\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"DivWheelsOffs\",\n" +
                "      \"dataType\": \"INT\",\n" +
                "      \"singleValueField\": false\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"DivWheelsOns\",\n" +
                "      \"dataType\": \"INT\",\n" +
                "      \"singleValueField\": false\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"Diverted\",\n" +
                "      \"dataType\": \"INT\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"FirstDepTime\",\n" +
                "      \"dataType\": \"INT\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"FlightDate\",\n" +
                "      \"dataType\": \"STRING\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"FlightNum\",\n" +
                "      \"dataType\": \"INT\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"Flights\",\n" +
                "      \"dataType\": \"INT\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"LateAircraftDelay\",\n" +
                "      \"dataType\": \"INT\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"LongestAddGTime\",\n" +
                "      \"dataType\": \"INT\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"Month\",\n" +
                "      \"dataType\": \"INT\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"NASDelay\",\n" +
                "      \"dataType\": \"INT\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"Origin\",\n" +
                "      \"dataType\": \"STRING\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"OriginAirportID\",\n" +
                "      \"dataType\": \"INT\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"OriginAirportSeqID\",\n" +
                "      \"dataType\": \"INT\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"OriginCityMarketID\",\n" +
                "      \"dataType\": \"INT\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"OriginCityName\",\n" +
                "      \"dataType\": \"STRING\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"OriginState\",\n" +
                "      \"dataType\": \"STRING\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"OriginStateFips\",\n" +
                "      \"dataType\": \"INT\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"OriginStateName\",\n" +
                "      \"dataType\": \"STRING\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"OriginWac\",\n" +
                "      \"dataType\": \"INT\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"Quarter\",\n" +
                "      \"dataType\": \"INT\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"RandomAirports\",\n" +
                "      \"dataType\": \"STRING\",\n" +
                "      \"singleValueField\": false\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"SecurityDelay\",\n" +
                "      \"dataType\": \"INT\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"TailNum\",\n" +
                "      \"dataType\": \"STRING\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"TaxiIn\",\n" +
                "      \"dataType\": \"INT\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"TaxiOut\",\n" +
                "      \"dataType\": \"INT\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"Year\",\n" +
                "      \"dataType\": \"INT\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"WheelsOn\",\n" +
                "      \"dataType\": \"INT\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"WheelsOff\",\n" +
                "      \"dataType\": \"INT\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"WeatherDelay\",\n" +
                "      \"dataType\": \"INT\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"UniqueCarrier\",\n" +
                "      \"dataType\": \"STRING\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"name\": \"TotalAddGTime\",\n" +
                "      \"dataType\": \"INT\"\n" +
                "    }\n" +
                "  ],\n" +
                "  \"timeFieldSpec\": {\n" +
                "    \"incomingGranularitySpec\": {\n" +
                "      \"name\": \"DaysSinceEpoch\",\n" +
                "      \"dataType\": \"INT\",\n" +
                "      \"timeType\": \"DAYS\"\n" +
                "    }\n" +
                "  },\n" +
                "  \"updateSemantic\": null\n" +
                "}");
    }

    @Override
    public TimeBoundary getTimeBoundaryForTable(String table)
    {
        if (TestPinotSplitManager.hybridTable.getTableName().equalsIgnoreCase(table)) {
            return new TimeBoundary("secondsSinceEpoch", "4562345");
        }

        return new TimeBoundary();
    }
}
