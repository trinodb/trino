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
package io.trino.delta;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.json.JsonBinder;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonModule;
import io.trino.block.BlockJsonSerde;
import io.trino.metadata.HandleJsonModule;
import io.trino.metadata.HandleResolver;
import io.trino.spi.block.Block;
import io.trino.spi.block.TestingBlockEncodingSerde;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.TestingTypeDeserializer;
import io.trino.spi.type.TestingTypeManager;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.type.TypeSignatureDeserializer;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static io.airlift.json.JsonBinder.jsonBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static io.trino.delta.DeltaColumnHandle.ColumnType.PARTITION;
import static io.trino.delta.DeltaColumnHandle.ColumnType.REGULAR;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static org.testng.Assert.assertEquals;

/**
 * Test {@link DeltaTableHandle} is created correctly with given arguments and JSON serialization/deserialization works.
 */
public class TestDeltaTableHandle
{
    @Test
    public void testJsonRoundTrip()
    {
        List<DeltaColumn> columns = ImmutableList.of(
                new DeltaColumn("c1", REAL.getTypeSignature(), true, true),
                new DeltaColumn("c2", INTEGER.getTypeSignature(), false, true),
                new DeltaColumn("c3", DOUBLE.getTypeSignature(), false, false),
                new DeltaColumn("c4", DATE.getTypeSignature(), true, false));

        DeltaTable deltaTable = new DeltaTable(
                "schema",
                "table",
                "s3:/bucket/table/location",
                Optional.of(1L),
                columns);

        DeltaColumnHandle c3ColumnHandle = new DeltaColumnHandle(
                "delta",
                columns.get(2).getName(),
                columns.get(2).getType(),
                columns.get(2).isPartition() ? PARTITION : REGULAR);

        TupleDomain<DeltaColumnHandle> predicate = TupleDomain.withColumnDomains(ImmutableMap.of(
                c3ColumnHandle, Domain.create(SortedRangeSet.copyOf(DOUBLE,
                                ImmutableList.of(
                                        Range.equal(DOUBLE, (double) (100.0f + 0)),
                                        Range.equal(DOUBLE, (double) (100.008f + 0)),
                                        Range.equal(DOUBLE, (double) (100.0f + 14)))),
                        false)));

        DeltaTableHandle expected = new DeltaTableHandle(
                "delta",
                deltaTable,
                predicate,
                Optional.of("predicateString"));

        String json = getJsonCodec().toJson(expected);
        DeltaTableHandle actual = getJsonCodec().fromJson(json);

        assertEquals(actual.getDeltaTable(), expected.getDeltaTable());
        assertEquals(actual.getPredicate(), expected.getPredicate());
        assertEquals(actual.getPredicateString(), expected.getPredicateString());
    }

    private JsonCodec<DeltaTableHandle> getJsonCodec()
    {
        Module module = binder -> {
            binder.install(new JsonModule());
            binder.install(new HandleJsonModule());

            TestingBlockEncodingSerde blockEncodingSerde = new TestingBlockEncodingSerde();

            JsonBinder jsonBinder = jsonBinder(binder);
            jsonBinder.addDeserializerBinding(Type.class).toInstance(new TestingTypeDeserializer(new TestingTypeManager()));
            jsonBinder.addSerializerBinding(Block.class).toInstance(new BlockJsonSerde.Serializer(blockEncodingSerde));
            jsonBinder.addDeserializerBinding(Block.class).toInstance(new BlockJsonSerde.Deserializer(blockEncodingSerde));
            jsonBinder.addDeserializerBinding(TypeSignature.class).to(TypeSignatureDeserializer.class);

            jsonCodecBinder(binder).bindJsonCodec(DeltaTableHandle.class);
        };
        Bootstrap app = new Bootstrap(ImmutableList.of(module));
        Injector injector = app
                .doNotInitializeLogging()
                .quiet()
                .initialize();
        HandleResolver handleResolver = injector.getInstance(HandleResolver.class);
        handleResolver.addCatalogHandleResolver("delta", new DeltaConnectionHandleResolver());
        return injector.getInstance(new Key<>()
        {
        });
    }
}
