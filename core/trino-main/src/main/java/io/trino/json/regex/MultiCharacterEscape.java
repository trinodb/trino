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
package io.trino.json.regex;

enum MultiCharacterEscape
        implements CharacterClassEscape
{
    WHITESPACE_CHARACTER_SEQUENCES,
    ALL_EXCEPT_SINGLE_CHARACTER_WHITESPACE,
    INITIAL_NAME_CHARACTERS,
    ALL_EXCEPT_INITIAL_NAME_CHARACTERS,
    NAME_CHARACTERS,
    ALL_EXCEPT_NAME_CHARACTERS,
    DECIMAL_DIGITS,
    ALL_EXCEPT_DECIMAL_DIGITS,
    ALL_EXCEPT_PUNCTUATION_SEPARATOR_AND_OTHER_CHARACTERS,
    PUNCTUATION_SEPARATOR_AND_OTHER_CHARACTERS;

    @Override
    public <R> R accept(RegexTreeVisitor<R> visitor)
    {
        return visitor.visitMultiCharacterEscape(this);
    }
}
