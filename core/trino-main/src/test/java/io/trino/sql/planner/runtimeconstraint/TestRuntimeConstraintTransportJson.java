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
package io.trino.sql.planner.runtimeconstraint;

import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.JsonMapperProvider;
import io.trino.block.BlockJsonSerde;
import io.trino.operator.RuntimeConstraintRequest;
import io.trino.spi.block.Block;
import io.trino.spi.predicate.Domain;
import io.trino.spi.type.Type;
import io.trino.sql.planner.plan.PlanFragmentId;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.planner.runtimeconstraint.RuntimeConstraintWiringReport.CollectedConstraint;
import io.trino.sql.planner.runtimeconstraint.RuntimeMembershipPayload.Lane;
import io.trino.type.TypeDeserializer;
import org.junit.jupiter.api.Test;

import static io.trino.metadata.InternalBlockEncodingSerde.TESTING_BLOCK_ENCODING_SERDE;
import static io.trino.spi.predicate.Domain.singleValue;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.sql.ir.ComparisonOperator.EQUAL;
import static io.trino.sql.ir.ComparisonOperator.GREATER_THAN;
import static io.trino.sql.ir.ComparisonOperator.LESS_THAN;
import static io.trino.sql.planner.runtimeconstraint.DistributedCompletionPolicy.UNION_ALL_PARTITIONS;
import static io.trino.sql.planner.runtimeconstraint.RuntimeConstraintNullMatchMode.ORDINARY;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;

public class TestRuntimeConstraintTransportJson
{
    private static final JsonCodecFactory CODEC_FACTORY;

    static {
        JsonMapper mapper = new JsonMapperProvider()
                .withJsonDeserializers(ImmutableMap.of(
                        Type.class, new TypeDeserializer(TESTING_TYPE_MANAGER),
                        Block.class, new BlockJsonSerde.Deserializer(TESTING_BLOCK_ENCODING_SERDE)))
                .withJsonSerializers(ImmutableMap.of(
                        Block.class, new BlockJsonSerde.Serializer(TESTING_BLOCK_ENCODING_SERDE)))
                .get();
        CODEC_FACTORY = new JsonCodecFactory(mapper);
    }

    @Test
    public void testContributionResponseRoundTrip()
    {
        RuntimeConstraintContribution contribution = new RuntimeConstraintContribution(
                new ProducerGroupId("group"),
                new ProducerBindingId("binding"),
                2,
                3,
                1,
                payload(11));
        RuntimeConstraintContributionBatch batch = new RuntimeConstraintContributionBatch(RuntimeConstraintProtocol.CURRENT_FORMAT_VERSION, 4, 7, ImmutableList.of(contribution));

        JsonCodec<RuntimeConstraintContributionBatch> codec = CODEC_FACTORY.jsonCodec(RuntimeConstraintContributionBatch.class);
        RuntimeConstraintContributionBatch copy = codec.fromJson(codec.toJson(batch));

        assertThat(copy).isEqualTo(batch);
    }

    @Test
    public void testUpdateBatchRoundTrip()
    {
        RuntimeConstraintId constraintId = new RuntimeConstraintId("constraint");
        RuntimeConstraintUpdateBatch batch = new RuntimeConstraintUpdateBatch(
                1,
                9,
                7,
                ImmutableList.of(RuntimeConstraintSnapshot.finalSnapshot(constraintId, 7, 1, payload(13))));

        JsonCodec<RuntimeConstraintUpdateBatch> codec = CODEC_FACTORY.jsonCodec(RuntimeConstraintUpdateBatch.class);
        String json = codec.toJson(batch);
        assertThat(json).doesNotContain("retainedSizeInBytes");
        assertThat(codec.fromJson(json)).isEqualTo(batch);
    }

    @Test
    public void testSharedLaneSourceRoundTrip()
    {
        RuntimeConstraintWiringReport.Source source = new RuntimeConstraintWiringReport.Source(
                new PlanNodeId("source"),
                ImmutableList.of(
                        new CollectedConstraint(new RuntimeConstraintId("greater"), GREATER_THAN, false, 1),
                        new CollectedConstraint(new RuntimeConstraintId("null_safe"), EQUAL, true, 0),
                        new CollectedConstraint(new RuntimeConstraintId("less"), LESS_THAN, false, 1)),
                ImmutableList.of(BIGINT, INTEGER),
                UNION_ALL_PARTITIONS);
        JsonCodec<RuntimeConstraintWiringReport.Source> codec = CODEC_FACTORY.jsonCodec(RuntimeConstraintWiringReport.Source.class);

        assertThat(codec.fromJson(codec.toJson(source))).isEqualTo(source);
    }

    @Test
    public void testSubscriptionGraphAndRemoteEndpointRoundTrip()
    {
        RuntimeConstraintId root = new RuntimeConstraintId("root");
        RuntimeConstraintSubscription first = RuntimeConstraintSubscription.create(root, root, "first", RuntimeConstraintTransform.comparison(LESS_THAN, false));
        RuntimeConstraintSubscription second = RuntimeConstraintSubscription.create(first.id(), root, "second", RuntimeConstraintTransform.cast(INTEGER));
        RuntimeConstraintRequest request = new RuntimeConstraintRequest(root, 0).withSubscription(second.id());
        RuntimeConstraintWiringReport report = new RuntimeConstraintWiringReport(
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableList.of(new RuntimeConstraintWiringReport.RemoteRequest(ImmutableList.of(new PlanFragmentId("upstream")), request)),
                ImmutableList.of(request),
                ImmutableList.of(),
                ImmutableList.of(first, second),
                ImmutableList.of(new RuntimeConstraintSubscription.Input(root, root)));
        JsonCodec<RuntimeConstraintWiringReport> codec = CODEC_FACTORY.jsonCodec(RuntimeConstraintWiringReport.class);

        assertThat(codec.fromJson(codec.toJson(report))).isEqualTo(report);
    }

    private static RuntimeMembershipPayload payload(long value)
    {
        return new RuntimeMembershipPayload(
                ImmutableList.of(new Lane(singleValue(BIGINT, value), false), new Lane(Domain.none(INTEGER), true)),
                ORDINARY,
                true);
    }
}
