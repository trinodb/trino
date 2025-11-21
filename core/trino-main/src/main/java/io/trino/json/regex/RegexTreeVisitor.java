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

abstract class RegexTreeVisitor<R>
{
    public R process(RegexNode node)
    {
        return node.accept(this);
    }

    protected R visitRegexNode(RegexNode node)
    {
        return null;
    }

    protected R visitRegularExpression(RegularExpression node)
    {
        return visitRegexNode(node);
    }

    protected R visitBranch(Branch node)
    {
        return visitRegexNode(node);
    }

    protected R visitPiece(Piece node)
    {
        return visitRegexNode(node);
    }

    protected R visitCharacterClassExpression(CharacterClassExpression node)
    {
        return visitRegexNode(node);
    }

    protected R visitNormalCharacter(NormalCharacter node)
    {
        return visitRegexNode(node);
    }

    protected R visitWildcardEscape(WildcardEscape node)
    {
        return visitRegexNode(node);
    }

    protected R visitEndMetaCharacter(EndMetaCharacter node)
    {
        return visitRegexNode(node);
    }

    protected R visitStartMetaCharacter(StartMetaCharacter node)
    {
        return visitRegexNode(node);
    }

    protected R visitCategoryEscape(CategoryEscape node)
    {
        return visitRegexNode(node);
    }

    protected R visitSingleCharacterEscape(SingleCharacterEscape node)
    {
        return visitRegexNode(node);
    }

    protected R visitMultiCharacterEscape(MultiCharacterEscape node)
    {
        return visitRegexNode(node);
    }

    protected R visitCharacterRange(CharacterRange node)
    {
        return visitRegexNode(node);
    }

    protected R visitSubExpression(SubExpression node)
    {
        return visitRegexNode(node);
    }

    protected R visitBackReference(BackReference node)
    {
        return visitRegexNode(node);
    }
}
