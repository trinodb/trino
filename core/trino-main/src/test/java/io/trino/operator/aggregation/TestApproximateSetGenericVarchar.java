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
package io.trino.operator.aggregation;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.nio.charset.StandardCharsets.UTF_8;

public class TestApproximateSetGenericVarchar
        extends AbstractTestApproximateSetGeneric
{
    @Override
    protected Type getValueType()
    {
        return VARCHAR;
    }

    @Override
    protected Object randomValue()
    {
        int length = ThreadLocalRandom.current().nextInt(100);

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            sb.append(Character.toString(ThreadLocalRandom.current().nextInt(1000)));
        }
        byte[] bytes = sb.toString().getBytes(UTF_8);

        return Slices.wrappedBuffer(bytes);
    }

    @Override
    protected List<Object> getResultStabilityTestSample()
    {
        return ImmutableList.of(
                Slices.utf8Slice(""),
                Slices.utf8Slice("ąęń∂∂ńńd"),
                Slices.utf8Slice("ala ma kota"),
                Slices.utf8Slice("fedfd fd fd fd f df df "),
                Slices.utf8Slice("aaaaaaaaaaaaaaaa"),
                Slices.utf8Slice("aaaaaaaaaaaa"),
                Slices.utf8Slice("aaaaaaaaaaaa"),
                Slices.utf8Slice("bbbb"),
                Slices.utf8Slice("some-non-bmp-\uD83D\uDE81"),
                Slices.utf8Slice("cccccc"));
    }

    @Override
    protected String getResultStabilityExpected()
    {
        // This value should not be changed; it is used to test result stability for $approx_set
        // Result stability is important because a produced value can be persisted by a connector.
        return "020C0900400F3E32C2EC5B7E80D0F8B7C04DD5B9C0B200C9807035D782C8F5DF00DB46EF012B5AFD";
    }
}
