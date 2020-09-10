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
package io.prestosql.pinot;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.pinot.client.PinotHostMapper;
import io.prestosql.pinot.util.TestingPinotCluster;
import io.prestosql.pinot.util.TestingPinotHostMapper;
import io.prestosql.testing.AbstractTestQueryFramework;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.QueryRunner;
import io.prestosql.testing.kafka.TestingKafka;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.inject.multibindings.OptionalBinder.newOptionalBinder;
import static io.airlift.testing.Closeables.closeAllRuntimeException;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestMinimalFunctionality
        extends AbstractTestQueryFramework
{
    public static final String TOPIC_AND_TABLE = "my_table";

    private TestingPinotCluster pinot;
    private TestingKafka kafka;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        kafka = new TestingKafka();
        kafka.start();
        pinot = new TestingPinotCluster();
        pinot.start();

        kafka.createTopic(TOPIC_AND_TABLE);

        ImmutableList.Builder<ProducerRecord<Long, Object>> builder = ImmutableList.builder();
        long key = 0L;
        builder.add(new ProducerRecord<>(TOPIC_AND_TABLE, key++, createTestRecord("vendor1", "Los Angeles", Arrays.asList("foo1", "bar1", "baz1"), Arrays.asList(5, 6, 7), Arrays.asList(3.5F, 5.5F), Arrays.asList(10_000.5D, 20_000.335D, -3.7D), Arrays.asList(10_000L, 20_000_000L, -37L), 4)));
        builder.add(new ProducerRecord<>(TOPIC_AND_TABLE, key++, createTestRecord("vendor2", "New York", Arrays.asList("foo2", "bar1", "baz1"), Arrays.asList(6, 7, 8), Arrays.asList(4.5F, 6.5F), Arrays.asList(10_000.5D, 20_000.335D, -3.7D), Arrays.asList(10_000L, 20_000_000L, -37L), 6)));
        builder.add(new ProducerRecord<>(TOPIC_AND_TABLE, key++, createTestRecord("vendor3", "Los Angeles", Arrays.asList("foo3", "bar2", "baz1"), Arrays.asList(7, 8, 9), Arrays.asList(5.5F, 7.5F), Arrays.asList(10_000.5D, 20_000.335D, -3.7D), Arrays.asList(10_000L, 20_000_000L, -37L), 8)));
        builder.add(new ProducerRecord<>(TOPIC_AND_TABLE, key++, createTestRecord("vendor4", "New York", Arrays.asList("foo4", "bar2", "baz2"), Arrays.asList(8, 9, 10), Arrays.asList(6.5F, 8.5F), Arrays.asList(10_000.5D, 20_000.335D, -3.7D), Arrays.asList(10_000L, 20_000_000L, -37L), 10)));
        builder.add(new ProducerRecord<>(TOPIC_AND_TABLE, key++, createTestRecord("vendor5", "Los Angeles", Arrays.asList("foo5", "bar3", "baz2"), Arrays.asList(9, 10, 11), Arrays.asList(7.5F, 9.5F), Arrays.asList(10_000.5D, 20_000.335D, -3.7D), Arrays.asList(10_000L, 20_000_000L, -37L), 12)));
        builder.add(new ProducerRecord<>(TOPIC_AND_TABLE, key++, createTestRecord("vendor6", "Los Angeles", Arrays.asList("foo6", "bar3", "baz2"), Arrays.asList(10, 11, 12), Arrays.asList(8.5F, 10.5F), Arrays.asList(10_000.5D, 20_000.335D, -3.7D), Arrays.asList(10_000L, 20_000_000L, -37L), 12)));
        builder.add(new ProducerRecord<>(TOPIC_AND_TABLE, key, createTestRecord("vendor7", "Los Angeles", Arrays.asList("foo6", "bar3", "baz2"), Arrays.asList(10, 11, 12), Arrays.asList(8.5F, 10.5F), Arrays.asList(10_000.5D, 20_000.335D, -3.7D), Arrays.asList(10_000L, 20_000_000L, -37L), 12)));
        produceRecords(builder.build());
        pinot.createSchema(getClass().getClassLoader().getResourceAsStream("schema.json"), TOPIC_AND_TABLE);
        pinot.addRealTimeTable(getClass().getClassLoader().getResourceAsStream("realtimeSpec.json"), TOPIC_AND_TABLE);

        Map<String, String> pinotProperties = ImmutableMap.<String, String>builder()
                .put("pinot.controller-urls", pinot.getControllerConnectString())
                .put("pinot.max-rows-per-split-for-segment-queries", "6")
                .build();

        return PinotQueryRunner.createPinotQueryRunner(
                ImmutableMap.of(),
                pinotProperties,
                Optional.of(binder -> newOptionalBinder(binder, PinotHostMapper.class).setBinding()
                        .toInstance(new TestingPinotHostMapper(pinot.getBrokerHostAndPort(), pinot.getServerHostAndPort()))));
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        closeAllRuntimeException(pinot, kafka);
    }

    @Test
    public void testRealType()
    {
        MaterializedResult result = computeActual("SELECT price FROM " + TOPIC_AND_TABLE + " WHERE vendor = 'vendor1'");
        assertEquals(getOnlyElement(result.getTypes()), REAL);
        assertEquals(result.getOnlyValue(), 3.5F);
    }

    @Test
    public void testIntegerType()
    {
        MaterializedResult result = computeActual("SELECT lucky_number FROM " + TOPIC_AND_TABLE + " WHERE vendor = 'vendor1'");
        assertEquals(getOnlyElement(result.getTypes()), INTEGER);
        assertEquals(result.getOnlyValue(), 5);
    }

    @Test
    public void testBrokerColumnMappingForSelectQueries()
    {
        String expected = "VALUES" +
                "  ('3.5', 'vendor1')," +
                "  ('4.5', 'vendor2')," +
                "  ('5.5', 'vendor3')," +
                "  ('6.5', 'vendor4')," +
                "  ('7.5', 'vendor5')," +
                "  ('8.5', 'vendor6')";
        assertQuery("SELECT price, vendor FROM \"SELECT price, vendor FROM " + TOPIC_AND_TABLE + " WHERE vendor != 'vendor7'\"", expected);
        assertQuery("SELECT price, vendor FROM \"SELECT * FROM " + TOPIC_AND_TABLE + " WHERE vendor != 'vendor7'\"", expected);
        assertQuery("SELECT price, vendor FROM \"SELECT vendor, lucky_numbers, price FROM " + TOPIC_AND_TABLE + " WHERE vendor != 'vendor7'\"", expected);
    }

    @Test
    public void testBrokerColumnMappingsForQueriesWithAggregates()
    {
        String passthroughQuery = "\"SELECT city, COUNT(*), MAX(price), SUM(lucky_number) " +
                "  FROM " + TOPIC_AND_TABLE +
                "  WHERE vendor != 'vendor7'" +
                "  GROUP BY city\"";
        assertQuery("SELECT * FROM " + passthroughQuery, "VALUES" +
                "  ('New York', 2, 6.5, 14)," +
                "  ('Los Angeles', 4, 8.5, 31)");
        assertQuery("SELECT \"max(price)\", city, \"sum(lucky_number)\", \"count(*)\" FROM " + passthroughQuery, "VALUES" +
                "  (6.5, 'New York', 14, 2)," +
                "  (8.5, 'Los Angeles', 31, 4)");
        assertQuery("SELECT \"max(price)\", city, \"count(*)\" FROM " + passthroughQuery, "VALUES" +
                "  (6.5, 'New York', 2)," +
                "  (8.5, 'Los Angeles', 4)");
    }

    @Test
    public void testBrokerColumnMappingsForArrays()
    {
        assertQuery("SELECT ARRAY_MIN(unlucky_numbers), ARRAY_MAX(long_numbers), ELEMENT_AT(neighbors, 2), ARRAY_MIN(lucky_numbers), ARRAY_MAX(prices)" +
                        "  FROM \"SELECT unlucky_numbers, long_numbers, neighbors, lucky_numbers, prices" +
                        "  FROM " + TOPIC_AND_TABLE +
                        "  WHERE vendor = 'vendor1'\"",
                "VALUES (-3.7, 20000000, 'bar1', 5, 5.5)");
        assertQuery("SELECT CARDINALITY(unlucky_numbers), CARDINALITY(long_numbers), CARDINALITY(neighbors), CARDINALITY(lucky_numbers), CARDINALITY(prices)" +
                        "  FROM \"SELECT unlucky_numbers, long_numbers, neighbors, lucky_numbers, prices" +
                        "  FROM " + TOPIC_AND_TABLE +
                        "  WHERE vendor = 'vendor1'\"",
                "VALUES (3, 3, 3, 3, 2)");
    }

    @Test
    public void testCountStarQueries()
    {
        assertQuery("SELECT COUNT(*) FROM \"SELECT * FROM " + TOPIC_AND_TABLE + " WHERE vendor != 'vendor7'\"", "VALUES(6)");
        assertQuery("SELECT COUNT(*) FROM " + TOPIC_AND_TABLE + " WHERE vendor != 'vendor7'", "VALUES(6)");
        assertQuery("SELECT \"count(*)\" FROM \"SELECT COUNT(*) FROM " + TOPIC_AND_TABLE + " WHERE vendor != 'vendor7'\"", "VALUES(6)");
    }

    @Test
    public void testBrokerQueriesWithAvg()
    {
        assertQuery("SELECT city, \"avg(lucky_number)\", \"avg(price)\", \"avg(long_number)\"" +
                "  FROM \"SELECT city, AVG(price), AVG(lucky_number), AVG(long_number) FROM my_table WHERE vendor != 'vendor7' GROUP BY city\"", "VALUES" +
                "  ('New York', 7.0, 5.5, 10000.0)," +
                "  ('Los Angeles', 7.75, 6.25, 10000.0)");
        MaterializedResult result = computeActual("SELECT \"avg(lucky_number)\"" +
                "  FROM \"SELECT AVG(lucky_number) FROM my_table WHERE vendor in ('vendor2', 'vendor4')\"");
        assertEquals(getOnlyElement(result.getTypes()), DOUBLE);
        assertEquals(result.getOnlyValue(), 7.0);
    }

    @Test
    public void testLimitForSegmentQueries()
    {
        assertQuerySucceeds("SELECT * FROM " + TOPIC_AND_TABLE + " WHERE vendor != 'vendor7'");
        assertQueryFails("SELECT * FROM " + TOPIC_AND_TABLE, "Segment query returned.*");
    }

    private static Object createTestRecord(
            String vendor,
            String city,
            List<String> neighbors,
            List<Integer> luckyNumbers,
            List<Float> prices,
            List<Double> unluckyNumbers,
            List<Long> longNumbers,
            long offset)
    {
        return new TestingRecord(vendor, city, neighbors, luckyNumbers, prices, unluckyNumbers, longNumbers, luckyNumbers.get(0), prices.get(0), unluckyNumbers.get(0), longNumbers.get(0), Instant.now().plusMillis(offset).getEpochSecond());
    }

    private static class TestingRecord
    {
        private final String vendor;
        private final String city;
        private final List<String> neighbors;
        private final List<Integer> luckyNumbers;
        private final List<Float> prices;
        private final List<Double> unluckyNumbers;
        private final List<Long> longNumbers;
        private final Integer luckyNumber;
        private final Float price;
        private final Double unluckyNumber;
        private final Long longNumber;
        private final long updatedAt;

        @JsonCreator
        public TestingRecord(
                @JsonProperty("vendor") String vendor,
                @JsonProperty("city") String city,
                @JsonProperty("neighbors") List<String> neighbors,
                @JsonProperty("lucky_numbers") List<Integer> luckyNumbers,
                @JsonProperty("prices") List<Float> prices,
                @JsonProperty("unlucky_numbers") List<Double> unluckyNumbers,
                @JsonProperty("long_numbers") List<Long> longNumbers,
                @JsonProperty("lucky_number") Integer luckyNumber,
                @JsonProperty("price") Float price,
                @JsonProperty("unlucky_number") Double unluckyNumber,
                @JsonProperty("long_number") Long longNumber,
                @JsonProperty("updatedAt") long updatedAt)
        {
            this.vendor = requireNonNull(vendor, "vendor is null");
            this.city = requireNonNull(city, "city is null");
            this.neighbors = requireNonNull(neighbors, "neighbors is null");
            this.luckyNumbers = requireNonNull(luckyNumbers, "luckyNumbers is null");
            this.prices = requireNonNull(prices, "prices is null");
            this.unluckyNumbers = requireNonNull(unluckyNumbers, "luckyNumbers is null");
            this.longNumbers = requireNonNull(longNumbers, "luckyNumbers is null");
            this.price = requireNonNull(price, "price is null");
            this.luckyNumber = requireNonNull(luckyNumber, "luckyNumber is null");
            this.unluckyNumber = requireNonNull(unluckyNumber, "unluckyNumber is null");
            this.longNumber = requireNonNull(longNumber, "longNumber is null");
            this.updatedAt = updatedAt;
        }

        @JsonProperty
        public String getVendor()
        {
            return vendor;
        }

        @JsonProperty
        public String getCity()
        {
            return city;
        }

        @JsonProperty
        public List<String> getNeighbors()
        {
            return neighbors;
        }

        @JsonProperty("lucky_numbers")
        public List<Integer> getLuckyNumbers()
        {
            return luckyNumbers;
        }

        @JsonProperty
        public List<Float> getPrices()
        {
            return prices;
        }

        @JsonProperty("unlucky_numbers")
        public List<Double> getUnluckyNumbers()
        {
            return unluckyNumbers;
        }

        @JsonProperty("long_numbers")
        public List<Long> getLongNumbers()
        {
            return longNumbers;
        }

        @JsonProperty("lucky_number")
        public Integer getLuckyNumber()
        {
            return luckyNumber;
        }

        @JsonProperty
        public Float getPrice()
        {
            return price;
        }

        @JsonProperty("unlucky_number")
        public Double getUnluckyNumber()
        {
            return unluckyNumber;
        }

        @JsonProperty("long_number")
        public Long getLongNumber()
        {
            return longNumber;
        }

        @JsonProperty
        public long getUpdatedAt()
        {
            return updatedAt;
        }
    }

    private void produceRecords(List<ProducerRecord<Long, Object>> records)
    {
        KafkaProducer<Long, Object> producer = kafka.createProducer();

        try {
            for (ProducerRecord<Long, Object> record : records) {
                producer.send(record);
            }
        }
        finally {
            producer.flush();
            producer.close();
        }
    }
}
