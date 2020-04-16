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
package io.prestosql.sql.parser;

import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.atn.ParserATNSimulator;
import org.antlr.v4.runtime.atn.PredictionContextCache;
import org.antlr.v4.runtime.dfa.DFA;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class ConcurrentSqlParser
        extends SqlBaseParser
{
    private static final AtomicReference<PredictionContextCache> predictionContextCache = new AtomicReference<>(new PredictionContextCache());
    private static final AtomicReference<DFA[]> decisionToDFA = new AtomicReference<>(Arrays.copyOf(_decisionToDFA, _decisionToDFA.length));
    private static final AtomicLong invocationCount = new AtomicLong();

    public ConcurrentSqlParser(TokenStream input)
    {
        super(input);
        if (invocationCount.incrementAndGet() % 10000 == 0) { // is it too often?
            clear();
        }
        _interp = new ParserATNSimulator(this, this.getATN(), decisionToDFA.get(), predictionContextCache.get());
    }

    private static void clear()
    {
        synchronized (decisionToDFA) {
            for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
                _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
            }
            predictionContextCache.set(new PredictionContextCache());
            decisionToDFA.set(Arrays.copyOf(_decisionToDFA, _decisionToDFA.length));
        }
    }
}
