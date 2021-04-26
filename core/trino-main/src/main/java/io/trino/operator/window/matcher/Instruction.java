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
package io.trino.operator.window.matcher;

interface Instruction
{
    enum Type
    {
        JUMP, MATCH_LABEL, MATCH_START, MATCH_END, SAVE, SPLIT, DONE
    }

    Type type();

    static Jump jump(int target)
    {
        return new Jump(target);
    }

    static Save save()
    {
        return new Save();
    }

    static Split split(int first, int second)
    {
        return new Split(first, second);
    }

    static Done done()
    {
        return new Done();
    }

    static MatchLabel match(int label)
    {
        return new MatchLabel(label);
    }

    static MatchStart matchStart()
    {
        return new MatchStart();
    }

    static MatchEnd matchEnd()
    {
        return new MatchEnd();
    }
}
