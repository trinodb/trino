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
package io.trino.plugin.druid;

import io.trino.spi.ErrorCode;
import org.testng.annotations.Test;

import static io.trino.plugin.druid.DruidErrorCode.DRUID_DDL_NOT_SUPPORTED;
import static io.trino.plugin.druid.DruidErrorCode.DRUID_DML_NOT_SUPPORTED;
import static io.trino.spi.ErrorType.EXTERNAL;
import static io.trino.testing.assertions.Assert.assertEquals;

public class TestDruidErrorCode
{
    @Test
    public void testErrorCodeEquals()
    {
        assertEquals(DRUID_DDL_NOT_SUPPORTED.toErrorCode(),
                new ErrorCode(0x0510_0000, DRUID_DDL_NOT_SUPPORTED.name(), EXTERNAL));
        assertEquals(DRUID_DML_NOT_SUPPORTED.toErrorCode(),
                new ErrorCode(1 + 0x0510_0000, DRUID_DML_NOT_SUPPORTED.name(), EXTERNAL));
    }
}
