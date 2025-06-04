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
package io.trino.operator;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.trino.operator.OutputSpoolingOperatorFactory.OutputSpoolingInfo;
import io.trino.operator.TableWriterOperator.TableWriterInfo;
import io.trino.operator.exchange.LocalExchangeBufferInfo;
import io.trino.operator.join.JoinOperatorInfo;
import io.trino.operator.output.PartitionedOutputOperator.PartitionedOutputInfo;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        property = "@type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = DirectExchangeClientStatus.class, name = "directExchangeClientStatus"),
        @JsonSubTypes.Type(value = JoinOperatorInfo.class, name = "joinOperatorInfo"),
        @JsonSubTypes.Type(value = LocalExchangeBufferInfo.class, name = "localExchangeBuffer"),
        @JsonSubTypes.Type(value = OutputSpoolingInfo.class, name = "outputSpooling"),
        @JsonSubTypes.Type(value = PartitionedOutputInfo.class, name = "partitionedOutput"),
        @JsonSubTypes.Type(value = TableFinishInfo.class, name = "tableFinish"),
        @JsonSubTypes.Type(value = TableWriterInfo.class, name = "tableWriter"),
        @JsonSubTypes.Type(value = WindowInfo.class, name = "windowInfo"),
})
public interface OperatorInfo
{
    /**
     * @return true if this OperatorInfo should be collected and sent to the coordinator when the task completes (i.e. it will not be stripped away during summarization).
     */
    default boolean isFinal()
    {
        return false;
    }
}
