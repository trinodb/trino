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
package io.trino.newir;

import io.trino.grammar.newir.NewIrBaseVisitor;
import io.trino.grammar.newir.NewIrParser;
import io.trino.newir.tree.AttributeNode;
import io.trino.newir.tree.BlockNode;
import io.trino.newir.tree.NewIrNode;
import io.trino.newir.tree.OperationNode;
import io.trino.newir.tree.ProgramNode;
import io.trino.newir.tree.RegionNode;
import io.trino.newir.tree.TypeNode;
import io.trino.sql.parser.ParsingException;
import org.antlr.v4.runtime.RuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.ParseTree;

import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.Integer.parseInt;

public class ProgramTreeBuilder
        extends NewIrBaseVisitor<NewIrNode>
{
    @Override
    public ProgramNode visitProgram(NewIrParser.ProgramContext context)
    {
        int version;
        String versionAsText = context.version.getText();
        if (versionAsText.startsWith("0")) {
            throw new ParsingException("invalid version. starts with '0': " + versionAsText, null, context.version.getLine(), context.version.getCharPositionInLine() + 1);
        }
        try {
            version = parseInt(versionAsText);
        }
        catch (NumberFormatException e) {
            throw new ParsingException("invalid version. not an integer: " + versionAsText, null, context.version.getLine(), context.version.getCharPositionInLine() + 1);
        }

        OperationNode root = (OperationNode) visit(context.operation());

        return new ProgramNode(version, root);
    }

    @Override
    public NewIrNode visitOperation(NewIrParser.OperationContext context)
    {
        return new OperationNode(
                Optional.ofNullable(context.operationName().dialectName()).map(RuleContext::getText),
                context.operationName().identifier().getText(),
                context.resultName.getText(),
                (TypeNode) visit(context.resultType),
                context.argumentNames.stream()
                        .map(Token::getText)
                        .collect(toImmutableList()),
                context.argumentTypes.stream()
                        .map(this::visit)
                        .map(TypeNode.class::cast)
                        .collect(toImmutableList()),
                context.region().stream()
                        .map(this::visit)
                        .map(RegionNode.class::cast)
                        .collect(toImmutableList()),
                context.attribute().stream()
                        .map(this::visit)
                        .map(AttributeNode.class::cast)
                        .collect(toImmutableList()));
    }

    @Override
    public NewIrNode visitType(NewIrParser.TypeContext context)
    {
        return new TypeNode(
                Optional.ofNullable(context.dialectName()).map(RuleContext::getText),
                context.STRING().getText());
    }

    @Override
    public NewIrNode visitRegion(NewIrParser.RegionContext context)
    {
        return new RegionNode(context.block().stream()
                .map(this::visit)
                .map(BlockNode.class::cast)
                .collect(toImmutableList()));
    }

    @Override
    public NewIrNode visitBlock(NewIrParser.BlockContext context)
    {
        return new BlockNode(
                Optional.ofNullable(context.BLOCK_NAME()).map(ParseTree::getText),
                context.blockParameter().stream()
                        .map(NewIrParser.BlockParameterContext::VALUE_NAME)
                        .map(ParseTree::getText)
                        .collect(toImmutableList()),
                context.blockParameter().stream()
                        .map(NewIrParser.BlockParameterContext::type)
                        .map(this::visit)
                        .map(TypeNode.class::cast)
                        .collect(toImmutableList()),
                context.operation().stream()
                        .map(this::visit)
                        .map(OperationNode.class::cast)
                        .collect(toImmutableList()));
    }

    @Override
    public NewIrNode visitAttribute(NewIrParser.AttributeContext context)
    {
        return new AttributeNode(
                Optional.ofNullable(context.attributeName().dialectName()).map(RuleContext::getText),
                context.attributeName().identifier().getText(),
                context.STRING().getText());
    }
}
