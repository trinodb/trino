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
package io.trino.plugin.varada.storage.read.fill;

import io.trino.plugin.varada.configuration.NativeConfiguration;
import io.trino.plugin.varada.storage.engine.StubsStorageEngineConstants;
import io.trino.plugin.warp.gen.constants.RecTypeCode;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class BlockFillersFactoryTest
{
    @Test
    public void test_real()
    {
        BlockFillersFactory blockFillersFactory = new BlockFillersFactory(
                new StubsStorageEngineConstants(),
                new NativeConfiguration());
        BlockFiller blockFiller = blockFillersFactory.getBlockFiller(RecTypeCode.REC_TYPE_REAL.ordinal());
        assertThat(blockFiller).isInstanceOf(IntBlockFiller.class);
    }
}
