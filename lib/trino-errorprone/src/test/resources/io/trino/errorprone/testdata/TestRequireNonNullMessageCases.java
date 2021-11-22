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
package io.trino.errorprone.testdata;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class TestRequireNonNullMessageCases
{
    private Object placeholder;

    public TestRequireNonNullMessageCases(Void fullPatternInConstructorWithoutStaticImport)
    {
        this.placeholder = Objects.requireNonNull(fullPatternInConstructorWithoutStaticImport, "fullPatternInConstructorWithoutStaticImport is null");
    }

    public TestRequireNonNullMessageCases(Byte bareIdentifierInConstructor)
    {
        this.placeholder = requireNonNull(bareIdentifierInConstructor, "bareIdentifierInConstructor");
    }

    public TestRequireNonNullMessageCases(Byte fullPatternInConstructor, int dummy)
    {
        this.placeholder = requireNonNull(fullPatternInConstructor, "fullPatternInConstructor is null");
    }

    public TestRequireNonNullMessageCases(Byte fullPatternWithInfixInConstructor, long dummy)
    {
        this.placeholder = requireNonNull(fullPatternWithInfixInConstructor, "fullPatternWithInfixInConstructor clearly is null");
    }

    public TestRequireNonNullMessageCases(Boolean fullPatternNestedInConstructor)
    {
        if (true == false) {
            for (String s : List.of("a", "b", "c")) {
                this.placeholder = requireNonNull(fullPatternNestedInConstructor, "fullPatternNestedInConstructor is null");
            }
        }
    }

    public TestRequireNonNullMessageCases(Integer messageMissingInConstructor)
    {
        // this is considered fine: just check the stack trace
        this.placeholder = requireNonNull(messageMissingInConstructor);
    }

    public TestRequireNonNullMessageCases(Long messageMalformedInConstructor)
    {
        // BUG: Diagnostic contains: RequireNonNullMessage
        // Did you mean 'this.placeholder = requireNonNull(messageMalformedInConstructor, "messageMalformedInConstructor is null");'?
        this.placeholder = requireNonNull(messageMalformedInConstructor, "messageMissingInConstructor is null");
    }

    public TestRequireNonNullMessageCases(Character messageMalformedNestedInConstructor)
    {
        if (true == false) {
            for (String s : List.of("a", "b", "c")) {
                // BUG: Diagnostic contains: RequireNonNullMessage
                // Did you mean 'this.placeholder = requireNonNull(messageMalformedNestedInConstructor, "messageMalformedNestedInConstructor is null");'?
                this.placeholder = requireNonNull(messageMalformedNestedInConstructor, "messageMissingInConstructor is null");
            }
        }
    }

    public TestRequireNonNullMessageCases(Float messageAlmostMalformedInConstructor)
    {
        // this is allowed in methods, but we don't like it in constructors
        // BUG: Diagnostic contains: RequireNonNullMessage
        // Did you mean 'this.placeholder = requireNonNull(messageAlmostMalformedInConstructor, "messageAlmostMalformedInConstructor is null");'?
        this.placeholder = requireNonNull(messageAlmostMalformedInConstructor, "my argument is null");
    }

    public TestRequireNonNullMessageCases(Double messageNotFittingPatternInConstructor)
    {
        // BUG: Diagnostic contains: RequireNonNullMessage
        // Did you mean 'this.placeholder = requireNonNull(messageNotFittingPatternInConstructor, "messageNotFittingPatternInConstructor is null");'?
        this.placeholder = requireNonNull(messageNotFittingPatternInConstructor, "I don't like this parameter");
    }

    public TestRequireNonNullMessageCases(String complexExpressionInConstructor)
    {
        // we only want to find typos and other simple cases, so ignore complex expressions
        this.placeholder = requireNonNull(getClass(), "I don't like this parameter");
    }

    public void fullPatternInMethodWithoutStaticImport(String parameter)
    {
        this.placeholder = Objects.requireNonNull(parameter, "parameter is null");
    }

    public void bareIdentifierInMethod(Object parameter)
    {
        this.placeholder = requireNonNull(parameter, "parameter");
    }

    public void fullPatternInMethod(Object parameter)
    {
        this.placeholder = requireNonNull(parameter, "parameter is null");
    }

    public void fullPatternWithInfixInMethod(Object parameter)
    {
        this.placeholder = requireNonNull(parameter, "parameter clearly is null");
    }

    public void fullPatternNestedInMethod(Object parameter)
    {
        if (true == false) {
            for (String s : List.of("a", "b", "c")) {
                this.placeholder = requireNonNull(parameter, "parameter is null");
            }
        }
    }

    public void messageMissingInMethod(String parameter)
    {
        // this is considered fine: just check the stack trace
        this.placeholder = requireNonNull(parameter);
    }

    public void messageMalformedInMethod(String parameter)
    {
        // BUG: Diagnostic contains: RequireNonNullMessage
        // Did you mean 'this.placeholder = requireNonNull(parameter, "parameter is null");'?
        this.placeholder = requireNonNull(parameter, "argument is null");
    }

    public void messageMalformedNestedInMethod(String parameter)
    {
        if (true == false) {
            for (String s : List.of("a", "b", "c")) {
                // BUG: Diagnostic contains: RequireNonNullMessage
                // Did you mean 'this.placeholder = requireNonNull(parameter, "parameter is null");'?
                this.placeholder = requireNonNull(parameter, "argument is null");
            }
        }
    }

    public void messageAlmostMalformedInMethod(String parameter)
    {
        // we're only flagging cases where the entire thing before the pattern looks like an identifier
        this.placeholder = requireNonNull(parameter, "my argument is null");
    }

    public void messageNotFittingPatternInMethod(String parameter)
    {
        // someone wanted to provide a more descriptive message, and this is fine
        this.placeholder = requireNonNull(parameter, "I don't like this parameter");
    }

    public void complexExpressionInMethod(String parameter)
    {
        // we only want to find typos and other simple cases, so ignore complex expressions
        this.placeholder = requireNonNull(parameter.getClass(), "I don't like this parameter");
    }
}
