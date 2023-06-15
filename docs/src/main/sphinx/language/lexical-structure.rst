=================
Lexical structure
=================

A SQL statement consist of a series of tokens, separated by whitespace (space,
tab, or a newline) characters, and terminated by a semicolon (";"). A token can
be:
* a keyword,
* an identifier,
* a quoted identifier,
* a literal (constant),
* or a special character symbol.

Statements could also contain comments, but they are not tokens, so they are
effectively ignored by Trino.

Example statement:

.. code-block:: text

    SELECT * FROM some_table WHERE a_column = '5';

The statement above include:
* ``SELECT``, ``FROM``, and ``WHERE`` tokens;
* A ``=`` operator, and ``*``, ``;`` special characters;
* ``some_table``, and ``a_column`` identifiers.

TODO: should we mentioned ``SqlBase.g4`` as the source of truth?

Identifiers and Key Words
-------------------------

Key word tokens have a fixed meaning in the SQL language. Token that identify
names of tables, column, functions, or other database objects, are identifiers.
Key words and identifiers have the same lexical structure, meaning that one
cannot know whether a token is an identifier or a key word without knowing the
language.

TODO: do we have a list of all key words?
TODO: translate: ``IDENTIFIER: (LETTER | '_') (LETTER | DIGIT | '_')*``
TODO: TIL there's support for backquoted identifiers, do we want to mention that too?
TODO: mention qualified identifiers (catalog.schema.name)?
TODO: case sensitivity
TODO: quoted identifiers (still not case sensitive)
TODO: unicode?

Literals
--------

String literals
^^^^^^^^^^^^^^^

Single quotes, escaping them.
Unicode Escapes

Numeric literals
^^^^^^^^^^^^^^^^

Other types
^^^^^^^^^^^

For example dates and timestamp

.. code-block:: text

    type 'string'
    CAST('string' AS type)


Operators
---------

```+ - * / < > = ~ ! @ # % ^ & | ` ?```

Special Characters
------------------

* Parentheses - ``()``
* Brackets - ``[]``
* Commas - ``,``
* Semicolon - ``;``
* Colon - ``:``
* Asterisk - ``*``
* Period - ``.``
* Underscore - ``_``

Comments
--------

Comments begin with double dashes and extend to the end of the line. Comments
can begin with ``/*`` and extend to the next occurrence of ``*/``, possibly
spanning over multiple lines.

.. code-block:: text

    -- This is a comment
    SELECT * FROM table; -- The rest of this line will be ignored

    /* This is a block comment
       that span multiple lines
       until it is closed */
