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
package io.trino.sql.ir;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/// A clause of [Match]: a single-argument predicate function over the match operand, paired
/// with the result expression that the [Match] returns when the predicate is true.
///
/// The predicate is either a plain [Lambda] or a [Bind] wrapping one — the latter form
/// arises when `LambdaCaptureDesugaringRewriter` lifts outer-scope references into explicit
/// captures. The lambda's single non-captured parameter binds the once-evaluated operand value;
/// the body decides whether this clause matches.
///
/// Bare-equality `CASE x WHEN v THEN r` clauses lower to `λp. p = v`; extended-CASE
/// clauses lower to the corresponding predicate body.
public record MatchClause(Expression predicate, Expression result)
{
    @JsonCreator
    public MatchClause(@JsonProperty("predicate") Expression predicate, @JsonProperty("result") Expression result)
    {
        this.predicate = requireNonNull(predicate, "predicate is null");
        this.result = requireNonNull(result, "result is null");
        checkArgument(predicate instanceof Lambda || predicate instanceof Bind,
                "predicate must be a Lambda or Bind, got %s",
                predicate.getClass().getSimpleName());
        Lambda lambda = predicate instanceof Bind bind ? bind.function() : (Lambda) predicate;
        int captureCount = predicate instanceof Bind bind ? bind.values().size() : 0;
        checkArgument(lambda.arguments().size() == captureCount + 1,
                "lambda must have one non-captured argument; got %s arguments with %s captures",
                lambda.arguments().size(),
                captureCount);
    }

    /// Returns the inner [Lambda], unwrapping a [Bind] if present. The returned lambda's
    /// argument list starts with any captured symbols (from the [Bind]'s `values`) and
    /// ends with the operand-bound parameter.
    public Lambda lambda()
    {
        return predicate instanceof Bind bind ? bind.function() : (Lambda) predicate;
    }

    /// Returns the [Bind] wrapping the lambda, if any. When present, the bind's `values`
    /// are bound to the leading arguments of [#lambda()] — the remaining argument is the
    /// operand parameter.
    public Bind bind()
    {
        return predicate instanceof Bind bind ? bind : null;
    }

    @Override
    public String toString()
    {
        return "When(%s, %s)".formatted(predicate, result);
    }
}
