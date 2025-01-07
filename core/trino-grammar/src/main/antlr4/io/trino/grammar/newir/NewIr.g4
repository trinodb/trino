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

grammar NewIr;

tokens {
    DELIMITER
}

program
    : IR VERSION EQ version=INTEGER_VALUE
        operation EOF
    ;

operation
    : resultName=VALUE_NAME EQ operationName
        '(' (argumentNames+=VALUE_NAME (',' argumentNames+=VALUE_NAME)*)? ')'
        ':' '(' (argumentTypes+=type (',' argumentTypes+=type)*)? ')'
        '->' resultType=type
        '(' (region (',' region)*)? ')'
        ('{' (attribute (',' attribute)*)? '}')? // does not roundtrip: we don't print empty attributes list // TODO test
    ;

region
    : '{' block+ '}'
    ;

block
    : BLOCK_NAME?
        ('(' blockParameter (',' blockParameter)* ')')?
        operation+
    ;

blockParameter
    : VALUE_NAME ':' type
    ;

attribute
    : attributeName EQ STRING
    ;

identifier
    : IDENTIFIER
    | nonReserved
    ;

dialectName
    : identifier
    ;

operationName
    : (dialectName '.')? identifier
    ;

attributeName
    : (dialectName '.')? identifier
    ;

type
    : (dialectName '.')? STRING
    ;

nonReserved
    : IR | VERSION
    ;

IR: 'IR';
VERSION: 'version';

EQ: '=';

STRING
    : '"' ( ~'"' | '""' )* '"'
    ;

VALUE_NAME
    : '%' PREFIXED_IDENTIFIER
    ;

BLOCK_NAME
    : '^' PREFIXED_IDENTIFIER
    ;

INTEGER_VALUE
    : DIGIT+
    ;

IDENTIFIER
    : (LETTER | '_') (LETTER | DIGIT | '_')*
    ;

PREFIXED_IDENTIFIER
    : (LETTER | DIGIT | '_')+
    ;

fragment DIGIT
    : [0-9]
    ;

fragment LETTER
    : [a-z] | [A-Z]
    ;

WS
    : [ \r\n\t]+ -> channel(HIDDEN)
    ;

// Catch-all for anything we can't recognize.
UNRECOGNIZED: .;
