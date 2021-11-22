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
package io.trino.errorprone;

import com.google.errorprone.BugPattern;
import com.google.errorprone.VisitorState;
import com.google.errorprone.bugpatterns.BugChecker;
import com.google.errorprone.bugpatterns.BugChecker.MethodInvocationTreeMatcher;
import com.google.errorprone.fixes.SuggestedFix;
import com.google.errorprone.matchers.Description;
import com.google.errorprone.matchers.Matcher;
import com.sun.source.tree.ExpressionTree;
import com.sun.source.tree.LiteralTree;
import com.sun.source.tree.MethodInvocationTree;
import com.sun.source.tree.MethodTree;
import com.sun.source.tree.Tree;

import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

import static com.google.errorprone.matchers.method.MethodMatchers.staticMethod;
import static com.google.errorprone.util.ASTHelpers.getSymbol;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

@BugPattern(
        summary = "An error message provided to Objects#requireNonNull is incorrect",
        severity = BugPattern.SeverityLevel.WARNING)
public class RequireNonNullMessage
        extends BugChecker
        implements MethodInvocationTreeMatcher
{
    private static final Pattern MESSAGE_PATTERN = Pattern.compile(" (?:(?:is|are|was|were) (?:null|empty|missing|none)|(?:is|are) required|(?:must not|cannot|can't) be (?:null|empty))");
    private static final Pattern IDENTIFIER_PATTERN = Pattern.compile("^\\p{javaUnicodeIdentifierStart}\\p{javaUnicodeIdentifierPart}*\\b");

    private static final Matcher<ExpressionTree> requireNonNull = staticMethod()
            .onClass(Objects.class.getName())
            .named("requireNonNull");

    @Override
    public Description matchMethodInvocation(MethodInvocationTree tree, VisitorState state)
    {
        if (!requireNonNull.matches(tree, state)) {
            return Description.NO_MATCH;
        }
        List<? extends ExpressionTree> arguments = tree.getArguments();
        if (arguments.isEmpty()) {
            // weird
            return Description.NO_MATCH;
        }

        // no error message at all:
        if (arguments.size() < 2) {
            // this is considered fine: just check the stack trace
            // (we could enable it at some point, but let's just focus on typos for now)
            return Description.NO_MATCH;
        }

        // the first argument: identifier or an expression?
        ExpressionTree objectArgument = arguments.get(0);
        if (objectArgument.getKind() != Tree.Kind.IDENTIFIER) {
            return Description.NO_MATCH;
        }
        String objectArgumentIdentifier = requireNonNull(state.getSourceForNode(objectArgument));

        // inspect the message
        ExpressionTree messageArgument = arguments.get(1);
        if (messageArgument.getKind() != Tree.Kind.STRING_LITERAL) {
            // something else than a literal: ignore
            // TODO: suggest using message supplier
            return Description.NO_MATCH;
        }
        String messageLiteral = (String) ((LiteralTree) messageArgument).getValue();

        java.util.regex.Matcher patternMatch = MESSAGE_PATTERN.matcher(messageLiteral);
        // note: there's no end of string anchor in the pattern, so we allow extra information at the end of the message
        // (and it starts with a space instead of the start of string anchor!)
        if (!patternMatch.find()) {
            // the message does not fit the pattern; if in a constructor, check for typos
            if (isWithinConstructor(state) && !messageLiteral.equals(objectArgumentIdentifier)) {
                return errorDescription(tree, (LiteralTree) messageArgument, objectArgumentIdentifier, " is null", "");
            }

            return Description.NO_MATCH;
        }

        java.util.regex.Matcher identifierMatch = IDENTIFIER_PATTERN.matcher(messageLiteral);
        if (!identifierMatch.find()) {
            // the thing before the pattern is not an identifier at all: ignore, but not in constructors
            if (isWithinConstructor(state)) {
                return errorDescription(tree, (LiteralTree) messageArgument, objectArgumentIdentifier, patternMatch.group(), messageLiteral.substring(patternMatch.end()));
            }

            return Description.NO_MATCH;
        }

        if (identifierMatch.end() != patternMatch.start()) {
            // the thing before the pattern is not only identifier;
            // if in a constructor, accept if the identifier matched is the same as the first argument (ignoring what follows it)
            // (e.g. 'requireNonNull(parameter, "parameter somehow is null")' is fine)
            if (isWithinConstructor(state) && !identifierMatch.group().equals(objectArgumentIdentifier)) {
                return errorDescription(tree, (LiteralTree) messageArgument, objectArgumentIdentifier, patternMatch.group(), messageLiteral.substring(patternMatch.end()));
            }

            return Description.NO_MATCH;
        }

        if (identifierMatch.group().equals(objectArgumentIdentifier)) {
            // not a typo: no error
            return Description.NO_MATCH;
        }

        // looks like something we're looking for: success!
        return errorDescription(tree, (LiteralTree) messageArgument, objectArgumentIdentifier, patternMatch.group(), messageLiteral.substring(patternMatch.end()));
    }

    private Description errorDescription(MethodInvocationTree tree, LiteralTree messageArgument, String objectArgumentIdentifier, String pattern, String suffix)
    {
        return describeMatch(
                tree,
                SuggestedFix.replace(
                        messageArgument,
                        format("\"%s%s%s\"", objectArgumentIdentifier, pattern, suffix)));
    }

    private boolean isWithinConstructor(VisitorState state)
    {
        MethodTree enclosingMethodTree = state.findEnclosing(MethodTree.class);
        return enclosingMethodTree != null && getSymbol(enclosingMethodTree).isConstructor();
    }
}
