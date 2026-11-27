# Design documents

These documents describe the motivation, contracts, and tradeoffs behind substantial
changes to Trino. They link to the user and developer documentation for contracts
defined there rather than repeating them.

- [Collations](collations.md): collated character string types, derivation of an
  expression's collation, and the optimizer rewrites that must not treat equal strings as
  identical.
- [Function preimages](function-preimages.md): shared function semantics for comparison
  rewriting, domain extraction, and dynamic filtering.
