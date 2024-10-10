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
package io.trino.plugin.pulsar;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.utility.MountableFile;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.time.temporal.ChronoUnit.SECONDS;
//import static java.util.UUID.randomUUID;

public class PulsarServer
            implements Closeable
{
    public static final String CUSTOMER = "customer";
    public static final String ORDERS = "orders";
    public static final String LINEITEM = "lineitem";
    public static final String NATION = "nation";
    public static final String REGION = "region";
    public static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    public static final int BROKER_HTTP_PORT = 8080;
    public static final int PULSAR_PORT = 6650;
    public static final int ZK_PORT = 2181;
    public static final int BK_PORT = 3181;
    public static final String DEFAULT_IMAGE_NAME = "apachepulsar/pulsar-all:4.0.0-preview.1";
    protected static final String SELECT_FROM_ORDERS = "SELECT " +
            "orderkey, " +
            "custkey, " +
            "orderstatus, " +
            "totalprice, " +
            "orderdate, " +
            "orderpriority, " +
            "clerk, " +
            "shippriority, " +
            "comment " +
            "FROM tpch.tiny.orders";
    protected static final String SELECT_FROM_LINEITEM = " SELECT " +
            "orderkey, " +
            "partkey, " +
            "suppkey, " +
            "linenumber, " +
            "quantity, " +
            "extendedprice, " +
            "discount, " +
            "tax, " +
            "returnflag, " +
            "linestatus, " +
            "shipdate, " +
            "commitdate, " +
            "receiptdate, " +
            "shipinstruct, " +
            "shipmode, " +
            "comment " +
            "FROM tpch.tiny.lineitem";
    protected static final String SELECT_FROM_NATION = " SELECT " +
            "nationkey, " +
            "name, " +
            "regionkey, " +
            "comment " +
            "FROM tpch.tiny.nation";
    protected static final String SELECT_FROM_REGION = " SELECT " +
            "regionkey, " +
            "name, " +
            "comment " +
            "FROM tpch.tiny.region";
    protected static final String SELECT_FROM_CUSTOMER = " SELECT " +
            "custkey, " +
            "name, " +
            "address, " +
            "nationkey, " +
            "phone, " +
            "acctbal, " +
            "mktsegment, " +
            "comment " +
            "FROM tpch.tiny.customer";
    private static final Logger log = Logger.get(PulsarServer.class);
    private final String hostWorkingDirectory;
    private final GenericContainer<?> pulsar;
    private final List<Consumer> consumers = new ArrayList<>();

    @SuppressWarnings("resource")
    public PulsarServer(String pulsarImage)
            throws IOException
    {
        hostWorkingDirectory = "/tpch"; //Files.createDirectory(Paths.get("/tpch")).toAbsolutePath().toString();
        File f = new File(hostWorkingDirectory);
        // Enable read/write/exec access for the services running in containers
        f.setWritable(true, false);
        f.setReadable(true, false);
        f.setExecutable(true, false);
        pulsar = new GenericContainer<>(pulsarImage)
                .withExposedPorts(BROKER_HTTP_PORT, ZK_PORT, PULSAR_PORT, BK_PORT)
                .withCommand("/pulsar/bin/pulsar standalone")
                .withCopyFileToContainer(MountableFile.forClasspathResource("/tpch/customer.json"), "/container-entrypoint-initdb.d/customer.json")
                .withCopyFileToContainer(MountableFile.forClasspathResource("/tpch/lineitem.json"), "/container-entrypoint-initdb.d/lineitem.json")
                .withCopyFileToContainer(MountableFile.forClasspathResource("/tpch/nation.json"), "/container-entrypoint-initdb.d/nation.json")
                .withCopyFileToContainer(MountableFile.forClasspathResource("/tpch/orders.json"), "/container-entrypoint-initdb.d/orders.json")
                .withCopyFileToContainer(MountableFile.forClasspathResource("/tpch/part.json"), "/container-entrypoint-initdb.d/part.json")
                .withCopyFileToContainer(MountableFile.forClasspathResource("/tpch/partsupp.json"), "/container-entrypoint-initdb.d/partsupp.json")
                .withCopyFileToContainer(MountableFile.forClasspathResource("/tpch/region.json"), "/container-entrypoint-initdb.d/region.json")
                .withCopyFileToContainer(MountableFile.forClasspathResource("/tpch/supplier.json"), "/container-entrypoint-initdb.d/supplier.json")
                //.withFileSystemBind(hostWorkingDirectory, "/opt/pulsar/var", BindMode.READ_WRITE)
                .waitingFor(new HttpWaitStrategy()
                        .forPort(BROKER_HTTP_PORT)
                        .forStatusCode(200)
                        .forPath("/admin/v2/namespaces/public/default")
                        .withStartupTimeout(java.time.Duration.of(600, SECONDS)));
        pulsar.setPortBindings(ImmutableList.of(String.format("%d:%d", BK_PORT, BK_PORT)));
        //Failsafe.with(CONTAINER_RETRY_POLICY).run(this::createContainer);
        pulsar.start();
    }

    private static <T> void sendMsgWithRetry(Producer<T> producer, Object data, String key, int retry, LongAdder counter)
    {
        if (retry > 0) {
            try {
                producer.newMessage().value((T) data).key(key).send();
                counter.increment();
            }
            catch (PulsarClientException e) {
                sendMsgWithRetry(producer, data, key, retry - 1, counter);
            }
        }
    }

    private static void writeTpchDataAsTsv(MaterializedResult rows, String dataFile)
            throws IOException
    {
        File file = new File(dataFile);
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(file))) {
            for (MaterializedRow row : rows.getMaterializedRows()) {
                bw.write(convertToTSV(row.getFields()));
                bw.newLine();
            }
        }
    }

    private static String convertToTSV(List<Object> data)
    {
        return data.stream()
                .map(String::valueOf)
                .collect(Collectors.joining("\t"));
    }

    public String getZKUrl()
    {
        return String.format("%s:%s", "localhost", pulsar.getMappedPort(ZK_PORT));
    }

    public String getPulsarAdminUrl()
    {
        return String.format("http://%s:%s", "localhost", pulsar.getMappedPort(BROKER_HTTP_PORT));
    }

    public String getPlainTextPulsarBrokerUrl()
    {
        return String.format("pulsar://%s:%s", "localhost", pulsar.getMappedPort(PULSAR_PORT));
    }

    public void copyAndIngestTpchData(MaterializedResult rows, String datasource, Class clazz, int partition)
            throws IOException, ParseException, PulsarAdminException
    {
        String tsvFileLocation = format("%s/%s.tsv", hostWorkingDirectory, datasource);
        writeTpchDataAsTsv(rows, tsvFileLocation);
        PulsarAdmin pulsarAdmin = PulsarAdmin.builder().serviceHttpUrl(getPulsarAdminUrl()).build();
        pulsarAdmin.topics().createPartitionedTopic(String.format("persistent://public/default/%s", datasource), partition);
        ingestData(tsvFileLocation, datasource, clazz, partition);
    }

    public <T> void ingestData(String inputTSV, String source, Class<T> clazz, int partition)
            throws IOException, ParseException
    {
        PulsarClient pulsarClient = PulsarClient.builder()
                .serviceUrl(getPlainTextPulsarBrokerUrl())
                .build();
        Schema schema = (new Random().nextInt()) % 2 == 0 ? JSONSchema.of(clazz) : AvroSchema.of(clazz);

        Producer<T> producer = pulsarClient.newProducer(schema)
                .compressionType(CompressionType.NONE)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.RoundRobinPartition)
                .topic(String.format("persistent://public/default/%s", source))
                .create();
        consumers.add(pulsarClient.newConsumer()
                .topic(String.format("persistent://public/default/%s", source))
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscriptionName("trino")
                .subscribe());
        BufferedReader reader = new BufferedReader(new FileReader(new File(inputTSV)));
        String line;
        LongAdder counter = new LongAdder();
        while ((line = reader.readLine()) != null) {
            if (source.equalsIgnoreCase(CUSTOMER)) {
                Customer c = toCustomer(line);
                sendMsgWithRetry(producer, c, "" + c.custkey, 3, counter);
            }
            else if (source.equalsIgnoreCase(ORDERS)) {
                Orders o = toOrders(line);
                sendMsgWithRetry(producer, o, "" + o.orderkey, 3, counter);
            }
            else if (source.equalsIgnoreCase(LINEITEM)) {
                LineItem l = toLineItem(line);
                sendMsgWithRetry(producer, l, "" + l.linenumber, 3, counter);
            }
            else if (source.equalsIgnoreCase(NATION)) {
                Nation n = toNation(line);
                sendMsgWithRetry(producer, n, "" + n.nationkey, 3, counter);
            }
            else if (source.equalsIgnoreCase(REGION)) {
                Region r = toRegion(line);
                sendMsgWithRetry(producer, r, "" + r.regionkey, 3, counter);
            }
        }
        // Pulsar Reader is not able to read the last message published in topic partition, so manually push 1 more message to each partition
        for (int i = 0; i < partition; i++) {
            if (source.equalsIgnoreCase(CUSTOMER)) {
                sendMsgWithRetry(producer, new Customer(), "" + i, 3, counter);
            }
            else if (source.equalsIgnoreCase(ORDERS)) {
                sendMsgWithRetry(producer, new Orders(), "" + i, 3, counter);
            }
            else if (source.equalsIgnoreCase(LINEITEM)) {
                sendMsgWithRetry(producer, new LineItem(), "" + i, 3, counter);
            }
            else if (source.equalsIgnoreCase(NATION)) {
                sendMsgWithRetry(producer, new Nation(), "" + i, 3, counter);
            }
            else if (source.equalsIgnoreCase(REGION)) {
                sendMsgWithRetry(producer, new Region(), "" + i, 3, counter);
            }
        }
    }

    private Customer toCustomer(String line)
    {
        String[] fields = line.split("\t");
        Customer customer = new Customer();
        customer.custkey = Long.parseLong(fields[0]);
        customer.name = fields[1];
        customer.address = fields[2];
        customer.nationkey = Long.parseLong(fields[3]);
        customer.phone = fields[4];
        customer.acctbal = Double.parseDouble(fields[5]);
        customer.mktsegment = fields[6];
        customer.comment = fields[7];
        return customer;
    }

    private Orders toOrders(String line)
            throws ParseException
    {
        String[] fields = line.split("\t");
        Orders orders = new Orders();
        orders.orderkey = Long.parseLong(fields[0]);
        orders.custkey = Long.parseLong(fields[1]);
        orders.orderstatus = fields[2];
        orders.totalprice = Double.parseDouble(fields[3]);
        orders.orderdate = LocalDate.parse(fields[4], formatter);
        orders.orderpriority = fields[5];
        orders.clerk = fields[6];
        orders.shippriority = Integer.parseInt(fields[7]);
        orders.comment = fields[8];
        return orders;
    }

    private LineItem toLineItem(String line)
            throws ParseException
    {
        String[] fields = line.split("\t");
        LineItem lineItem = new LineItem();
        lineItem.orderkey = Long.parseLong(fields[0]);
        lineItem.partkey = Long.parseLong(fields[1]);
        lineItem.suppkey = Long.parseLong(fields[2]);
        lineItem.linenumber = Integer.parseInt(fields[3]);
        lineItem.quantity = Double.parseDouble(fields[4]);
        lineItem.extendedprice = Double.parseDouble(fields[5]);
        lineItem.discount = Double.parseDouble(fields[6]);
        lineItem.tax = Double.parseDouble(fields[7]);
        lineItem.returnflag = fields[8];
        lineItem.linestatus = fields[9];
        lineItem.shipdate = LocalDate.parse(fields[10], formatter);
        lineItem.commitdate = LocalDate.parse(fields[11], formatter);
        lineItem.receiptdate = LocalDate.parse(fields[12], formatter);
        lineItem.shipinstruct = fields[13];
        lineItem.shipmode = fields[14];
        lineItem.comment = fields[15];
        return lineItem;
    }

    private Nation toNation(String line)
    {
        String[] fields = line.split("\t");
        Nation nation = new Nation();
        nation.nationkey = Long.parseLong(fields[0]);
        nation.name = fields[1];
        nation.regionkey = Long.parseLong(fields[2]);
        nation.comment = fields[3];
        return nation;
    }

    private Region toRegion(String line)
    {
        String[] fields = line.split("\t");
        Region region = new Region();
        region.regionkey = Long.parseLong(fields[0]);
        region.name = fields[1];
        region.comment = fields[2];
        return region;
    }

    @Override
    public void close()
    {
        pulsar.close();
        for (Consumer consumer : consumers) {
            try {
                consumer.close();
            }
            catch (PulsarClientException e) {
            }
        }
    }

    public static class LocalDateSerializer
            extends StdSerializer<LocalDate>
    {
        private static final long serialVersionUID = 1L;

        public LocalDateSerializer()
        {
            super(LocalDate.class);
        }

        @Override
        public void serialize(LocalDate value, JsonGenerator gen, SerializerProvider sp)
                throws IOException
        {
            gen.writeNumber(value.toEpochDay());
        }
    }

    public static class Customer
    {
        public long custkey;
        public String name;
        public String address;
        public long nationkey;
        public String phone;
        public double acctbal;
        public String mktsegment;
        public String comment;
    }

    public static class Orders
    {
        public long orderkey;
        public long custkey;
        public String orderstatus;
        public double totalprice;
        @org.apache.avro.reflect.AvroSchema("{\"type\":\"int\",\"logicalType\":\"date\"}")
        @JsonSerialize(using = LocalDateSerializer.class)
        public LocalDate orderdate;
        public String orderpriority;
        public String clerk;
        public int shippriority;
        public String comment;
    }

    public static class LineItem
    {
        public long orderkey;
        public long partkey;
        public long suppkey;
        public int linenumber;
        public double quantity;
        public double extendedprice;
        public double discount;
        public double tax;
        public String returnflag;
        public String linestatus;
        @org.apache.avro.reflect.AvroSchema("{ \"type\": \"int\", \"logicalType\": \"date\" }")
        @JsonSerialize(using = LocalDateSerializer.class)
        public LocalDate shipdate;
        @org.apache.avro.reflect.AvroSchema("{ \"type\": \"int\", \"logicalType\": \"date\" }")
        @JsonSerialize(using = LocalDateSerializer.class)
        public LocalDate commitdate;
        @org.apache.avro.reflect.AvroSchema("{ \"type\": \"int\", \"logicalType\": \"date\" }")
        @JsonSerialize(using = LocalDateSerializer.class)
        public LocalDate receiptdate;
        public String shipinstruct;
        public String shipmode;
        public String comment;
    }

    public static class Nation
    {
        public long nationkey;
        public String name;
        public long regionkey;
        public String comment;
    }

    public static class Region
    {
        public long regionkey;
        public String name;
        public String comment;
    }
}
