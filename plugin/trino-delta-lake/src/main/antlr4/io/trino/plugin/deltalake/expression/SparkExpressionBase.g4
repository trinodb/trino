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

grammar SparkExpressionBase;

tokens {
    DELIMITER
}

standaloneExpression
    : expression EOF
    ;

expression
    : booleanExpression
    ;

booleanExpression
    : valueExpression predicate[$valueExpression.ctx]?   #predicated
    | left=booleanExpression AND right=booleanExpression #and
    | left=booleanExpression OR right=booleanExpression #or
    ;

// TODO: Support LIKE clause and function calls
// workaround for https://github.com/antlr/antlr4/issues/780
predicate[ParserRuleContext value]
    : comparisonOperator right=valueExpression          #comparison
    ;

valueExpression
    : primaryExpression                                 #valueExpressionDefault
    ;

primaryExpression
    : number                                            #numericLiteral
    | booleanValue                                      #booleanLiteral
    | string                                            #stringLiteral
    | identifier                                        #columnReference
    ;

// TODO: Support raw string literal
string
    : STRING                                            #unicodeStringLiteral
    ;

comparisonOperator
    : EQ | NEQ | LT | LTE | GT | GTE
    ;

booleanValue
    : TRUE | FALSE
    ;

// "..." is a varchar literal in Spark SQL, not a quoted identifier
identifier
    : IDENTIFIER             #unquotedIdentifier
    | BACKQUOTED_IDENTIFIER  #backQuotedIdentifier
    ;

// TODO: Support decimals and scientific notation
number
    : MINUS? INTEGER_VALUE  #integerLiteral
    ;

AND: 'AND';
OR: 'OR';
FALSE: 'FALSE';
TRUE: 'TRUE';

EQ: '=';
NEQ: '<>' | '!=';
LT: '<';
LTE: '<=';
GT: '>';
GTE: '>=';

MINUS: '-';

STRING
    : '\'' ( ~'\'' | '\'\'' )* '\''
    | '"' ( ~'"' | '""' )* '"'
    ;

INTEGER_VALUE
    : DIGIT+
    ;

IDENTIFIER
    : (LETTER | '_') (LETTER | DIGIT | '_')*
    ;

BACKQUOTED_IDENTIFIER
    : '`' ( ~'`' | '``' )* '`'
    ;

fragment DIGIT
    : [0-9]
    ;

fragment LETTER
    : [A-Z]
    ;

WS
    : [ \r\n\t]+ -> channel(HIDDEN)
    ;
