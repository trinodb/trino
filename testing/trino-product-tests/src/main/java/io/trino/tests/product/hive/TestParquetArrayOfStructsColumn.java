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
package io.trino.tests.product.hive;

import com.google.inject.Inject;
import io.trino.tempto.AfterTestWithContext;
import io.trino.tempto.BeforeTestWithContext;
import io.trino.tempto.hadoop.hdfs.HdfsClient;
import org.testng.annotations.Test;

import java.nio.file.Paths;

import static io.trino.tempto.assertions.QueryAssert.Row.row;
import static io.trino.tempto.assertions.QueryAssert.assertThat;
import static io.trino.tests.product.TestGroups.PARQUET;
import static io.trino.tests.product.TestGroups.STORAGE_FORMATS;
import static io.trino.tests.product.utils.QueryExecutors.onTrino;

public class TestParquetArrayOfStructsColumn
        extends HiveProductTest
{
    @Inject
    private HdfsClient hdfsClient;

    @BeforeTestWithContext
    public void setup()
            throws Exception
    {
        hdfsClient.createDirectory("/user/hive/warehouse/TestParquetArrayOfStructsColumn/events");
        saveOnHdfs(hdfsClient,
                Paths.get("/docker/presto-product-tests/parquet/events.parquet"),
                "/user/hive/warehouse/TestParquetArrayOfStructsRow/events/events.parquet");

        hdfsClient.createDirectory("/user/hive/warehouse/TestParquetArrayOfStructsColumn/events-proto");
        saveOnHdfs(hdfsClient,
                Paths.get("/docker/presto-product-tests/parquet/events-proto-repeated-group.parquet"),
                "/user/hive/warehouse/TestParquetArrayOfStructsRow/events-proto/events-proto-repeated-group.parquet");
    }

    @AfterTestWithContext
    public void cleanup()
    {
        hdfsClient.delete("/user/hive/warehouse/TestParquetArrayOfStructsColumn");
    }

    @Test(groups = {PARQUET, STORAGE_FORMATS})
    public void testSelectArrayOfStructsColumn()
    {
        /*
        message custom_schema {
          optional binary id (STRING);
          optional group entries (LIST) {
            repeated group list {
              optional group element {
                optional binary name (STRING);
                optional group before {
                  optional binary value (STRING); }
                optional group after {
                  optional binary value (STRING); }}}}}
         */
        onTrino().executeQuery("CREATE TABLE events (\n" +
                "entries ARRAY(ROW(name VARCHAR, before ROW(value VARCHAR), after ROW(value VARCHAR))))\n" +
                "WITH (\n" +
                "external_location = 'hdfs:///user/hive/warehouse/TestParquetArrayOfStructsRow/events',\n" +
                "format = 'PARQUET');");

        // TODO Match array content, not just length (requires support from tempto).
        /*
        Row expected = row(
                "{name=test-entry-0, before={value=before-0}, after={value=after-0}}",
                "{name=test-entry-1, before={value=before-1}, after={value=after-1}}",
                "{name=test-entry-2, before={value=before-2}, after={value=after-2}}");
         */
        assertThat(onTrino().executeQuery("SELECT CARDINALITY(entries) as size FROM events"))
                .containsExactlyInOrder(row(3));

        onTrino().executeQuery("DROP TABLE events");
    }

    @Test(groups = {PARQUET, STORAGE_FORMATS})
    public void testSelectArrayOfStructsColumn_ProtoRepeatedGroup()
    {
        // The parquet in events-proto-repeated-group.parquet was written from protobuf using schema below.
        // Note the 'repeated group' - it does not get parsed successfully as parquet format requires a
        // wrapper of type LIST and a repeated group inside, as in `testSelectArrayOfStructsColumn()`.
        /*
        message Event {
          optional binary id (STRING) = 1;
          repeated group entries = 2 {
            optional binary name (STRING) = 3;
            optional group before = 4 {
              optional binary value (STRING) = 5; }
            optional group after = 6 {
              optional binary value (STRING) = 7; }}}
         */
        onTrino().executeQuery("CREATE TABLE events_proto (\n" +
                "entries ARRAY(ROW(name VARCHAR, before ROW(value VARCHAR), after ROW(value VARCHAR))))\n" +
                "WITH (\n" +
                "external_location = 'hdfs:///user/hive/warehouse/TestParquetArrayOfStructsRow/events-proto',\n" +
                "format = 'PARQUET');");

        assertThat(onTrino().executeQuery("SELECT entries FROM events_proto"))
                .containsExactlyInOrder(row((Object) null));

        onTrino().executeQuery("DROP TABLE events_proto");
    }
}
