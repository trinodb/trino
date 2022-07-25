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
package io.trino.sql.parser;

import io.trino.sql.tree.NodeLocation;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestParsingException
{
    @Test
    public void test()
    {
        ParsingException exception = new ParsingException("the message", new NodeLocation(5, 13));

        assertThat(exception)
                .hasMessage("line 5:13: the message");

        assertThat(exception.getLineNumber())
                .isEqualTo(5);

        assertThat(exception.getColumnNumber())
                .isEqualTo(13);
    }
}
