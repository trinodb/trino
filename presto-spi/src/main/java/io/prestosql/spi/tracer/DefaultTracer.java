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
package io.prestosql.spi.tracer;

import io.prestosql.spi.eventlistener.TracerEvent;

import java.net.URI;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

// Tracer keeps track of Presto execution unit contexts and sends debugging purpose events via its emitter.
// Users should not build complex metrics based on the action events.
public class DefaultTracer
        implements Tracer
{
    private final Consumer<TracerEvent> emitter;
    private final Function<Map<String, Object>, String> payloadEncoder;
    // presto coordinator or worker node emitting tracer events
    private final String nodeId;
    private final URI uri;
    private final String queryId;
    private final Optional<String> stageId;
    private final Optional<String> taskId;
    private final Optional<String> pipelineId;
    private final Optional<String> splitId;
    private final Optional<String> operatorId;

    private DefaultTracer(
            Consumer<TracerEvent> emitter,
            Function<Map<String, Object>, String> payloadEncoder,
            String nodeId,
            URI uri,
            String queryId,
            Optional<String> stageId,
            Optional<String> taskId,
            Optional<String> pipelineId,
            Optional<String> splitId,
            Optional<String> operatorId)
    {
        this.emitter = requireNonNull(emitter, "emitter is null");
        this.payloadEncoder = requireNonNull(payloadEncoder, "payloadEncoder is null");
        this.nodeId = requireNonNull(nodeId, "nodeId is null");
        this.uri = requireNonNull(uri, "uri is null");
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.stageId = requireNonNull(stageId, "stageId is null");
        this.taskId = requireNonNull(taskId, "taskId is null");
        this.pipelineId = requireNonNull(pipelineId, "pipelineId is null");
        this.splitId = requireNonNull(splitId, "splitId is null");
        this.operatorId = requireNonNull(operatorId, "operatorId is null");
    }

    public static Tracer createBasicTracer(Consumer<TracerEvent> emitter, Function<Map<String, Object>, String> payloadEncoder, String nodeId, URI uri, String queryId)
    {
        return Builder.build(emitter, payloadEncoder, nodeId, uri, queryId).build();
    }

    @Override
    public Tracer withStageId(String stageId)
    {
        return Builder.build(this).withStageId(stageId).build();
    }

    @Override
    public Tracer withTaskId(String taskId)
    {
        return Builder.build(this).withTaskId(taskId).build();
    }

    @Override
    public Tracer withPipelineId(String pipelineId)
    {
        return Builder.build(this).withPipelineId(pipelineId).build();
    }

    @Override
    public Tracer withSplitId(String splitId)
    {
        return Builder.build(this).withSplitId(splitId).build();
    }

    @Override
    public Tracer withOperatorId(String operatorId)
    {
        return Builder.build(this).withOperatorId(operatorId).build();
    }

    public Consumer<TracerEvent> getEmitter()
    {
        return emitter;
    }

    public Function<Map<String, Object>, String> getPayloadEncoder()
    {
        return payloadEncoder;
    }

    public String getNodeId()
    {
        return nodeId;
    }

    public URI getUri()
    {
        return uri;
    }

    public String getQueryId()
    {
        return queryId;
    }

    public Optional<String> getStageId()
    {
        return stageId;
    }

    public Optional<String> getTaskId()
    {
        return taskId;
    }

    public Optional<String> getPipelineId()
    {
        return pipelineId;
    }

    public Optional<String> getSplitId()
    {
        return splitId;
    }

    public Optional<String> getOperatorId()
    {
        return operatorId;
    }

    @Override
    public void emitEvent(TracerEventTypeSupplier eventTypeSupplier, PayloadBuilder payloadBuilder)
    {
        String uriString = uri.getHost() == null ? null : uri.getHost() + ":" + String.valueOf(uri.getPort());

        TracerEvent event = new TracerEvent(
                nodeId,
                uriString,
                queryId,
                stageId,
                taskId,
                pipelineId,
                splitId,
                operatorId,
                Thread.currentThread().getName(),
                Instant.now(),
                eventTypeSupplier.toTracerEventType(),
                payloadBuilder == null ? Optional.empty() : Optional.of(payloadEncoder.apply(payloadBuilder.getPayload())));
        emitter.accept(event);
    }

    private static class Builder
    {
        private final Consumer<TracerEvent> emitter;
        private final Function<Map<String, Object>, String> payloadEncoder;
        private final String nodeId;
        private final URI uri;
        private String queryId;
        private Optional<String> stageId = Optional.empty();
        private Optional<String> taskId = Optional.empty();
        private Optional<String> pipelineId = Optional.empty();
        private Optional<String> splitId = Optional.empty();
        private Optional<String> operatorId = Optional.empty();

        private Builder(Consumer<TracerEvent> emitter, Function<Map<String, Object>, String> payloadEncoder, String nodeId, URI uri, String queryId)
        {
            this.emitter = requireNonNull(emitter, "emitter is null");
            this.payloadEncoder = requireNonNull(payloadEncoder, "payloadEncoder is null");
            this.nodeId = requireNonNull(nodeId, "queryId is null");
            this.uri = requireNonNull(uri, "uri is null");
            this.queryId = requireNonNull(queryId, "queryId is null");
        }

        private Builder(DefaultTracer tracer)
        {
            requireNonNull(tracer, "tracer is null");
            this.emitter = tracer.getEmitter();
            this.payloadEncoder = tracer.getPayloadEncoder();
            this.nodeId = tracer.getNodeId();
            this.uri = tracer.getUri();
            this.queryId = tracer.getQueryId();
            this.stageId = tracer.getStageId();
            this.taskId = tracer.getTaskId();
            this.pipelineId = tracer.getPipelineId();
            this.splitId = tracer.getSplitId();
        }

        public Builder withStageId(String stageId)
        {
            requireNonNull(stageId, "stageId is null");
            this.stageId = Optional.of(stageId);
            return this;
        }

        public Builder withTaskId(String taskId)
        {
            requireNonNull(taskId, "taskId is null");
            this.taskId = Optional.of(taskId);
            return this;
        }

        public Builder withPipelineId(String pipelineId)
        {
            requireNonNull(pipelineId, "pipelineId is null");
            this.pipelineId = Optional.of(pipelineId);
            return this;
        }

        public Builder withSplitId(String splitId)
        {
            requireNonNull(splitId, "splitId is null");
            this.splitId = Optional.of(splitId);
            return this;
        }

        public Builder withOperatorId(String operatorId)
        {
            requireNonNull(operatorId, "operatorId is null");
            this.operatorId = Optional.of(operatorId);
            return this;
        }

        public Tracer build()
        {
            return new DefaultTracer(emitter, payloadEncoder, nodeId, uri, queryId, stageId, taskId, pipelineId, splitId, operatorId);
        }

        public static Builder build(Consumer<TracerEvent> emitter, Function<Map<String, Object>, String> payloadEncoder, String nodeId, URI uri, String queryId)
        {
            return new Builder(emitter, payloadEncoder, nodeId, uri, queryId);
        }

        public static Builder build(DefaultTracer tracer)
        {
            return new Builder(tracer);
        }
    }
}
