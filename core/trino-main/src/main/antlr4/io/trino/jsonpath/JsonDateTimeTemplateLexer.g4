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

// Tokens for SQL/JSON path datetime() format templates per ISO/IEC 9075-2:2023 §9.46.
// Templates have no nesting or precedence, so a lexer with no parser rules suffices —
// the consumer iterates over the token stream directly.
lexer grammar JsonDateTimeTemplateLexer;

options { caseInsensitive = true; }

YYYY : 'YYYY';
YYY  : 'YYY';
YY   : 'YY';
Y    : 'Y';

RRRR : 'RRRR';
RR   : 'RR';

MM   : 'MM';
DDD  : 'DDD';
DD   : 'DD';

HH24 : 'HH24';
HH12 : 'HH12';
HH   : 'HH';

MI    : 'MI';
SSSSS : 'SSSSS';
SS    : 'SS';

// FF1..FF9 are the standard widths; FF10..FF12 extend coverage to Trino's
// maximum TIME / TIMESTAMP precision of 12.
FRACTION : 'FF' [0-9] [0-9]?;

AM_PM : ('A' | 'P') '.M.';

TZH : 'TZH';
TZM : 'TZM';

DELIMITER
    : '-' | '.' | '/' | ',' | '\'' | ';' | ':' | ' '
    ;

// Double-quoted literal text (e.g. `YYYY"T"HH24:MI:SS`). An embedded quote is
// escaped as `""`. The unescape step is performed in the consumer.
QUOTED_LITERAL
    : '"' ('""' | ~'"')* '"'
    ;
