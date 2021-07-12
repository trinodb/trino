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
package io.trino.cli;

import org.jline.reader.EOFError;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.jline.reader.Parser.ParseContext.ACCEPT_LINE;
import static org.jline.reader.Parser.ParseContext.COMPLETE;
import static org.testng.Assert.assertNotNull;

public class TestInputParser
{
    @Test
    public void testParseIncompleteStatements()
    {
        InputParser instance = new InputParser();
        // assert an incomplete statement throws an error if the ParseContext is not COMPLETE
        assertThatThrownBy(() -> instance.parse("show", 4, ACCEPT_LINE)).isInstanceOf(EOFError.class);
        assertNotNull(instance.parse("show", 4, COMPLETE));
    }
}
