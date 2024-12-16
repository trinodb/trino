# Example Python UDFs

After learning about [](/udf/python), the following sections show examples
of valid Python UDFs. 

## XOR

The following example implements a `xor` function for a logical Exclusive OR
operation on two boolean input parameters and tests it with two invocations:

```text
WITH FUNCTION xor(a boolean, b boolean)
RETURNS boolean
LANGUAGE PYTHON
WITH (handler = 'bool_xor')
AS $$
import operator
def bool_xor(a, b):
    return operator.xor(a, b)
$$
SELECT xor(true, false), xor(false, true);
```

Result of the query:

```
 true  | true
```

## reverse_words

The following example uses a more elaborate Python script to reverse the
characters in each word of the input string `s` of type `varchar` and tests the
function.

```text
WITH FUNCTION reverse_words(s varchar)
RETURNS varchar
LANGUAGE PYTHON
WITH (handler = 'reverse_words')
AS $$
import re

def reverse(s):
    str = ""
    for i in s:
        str = i + str
    return str

pattern = re.compile(r"\w+[.,'!?\"]\w*")

def process_word(word):
    # Reverse only words without non-letter signs
    return word if pattern.match(word) else reverse(word)

def reverse_words(payload):
    text_words = payload.split(' ')
    return ' '.join([process_word(w) for w in text_words])
$$
SELECT reverse_words('Civic, level, dna racecar era semordnilap');
```

Result of the query:

```
Civic, level, and racecar are palindromes
```