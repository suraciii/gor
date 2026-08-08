# Documentation Language and Style

This document is the writing rule for gor documentation.

## Standard

Use [ASD-STE100 Simplified Technical English, Issue
9](https://www.asd-ste100.org/). It has writing rules and a controlled
dictionary. It selects simple words and gives each word a clear meaning.

Use [ISO 24495-1:2023 Plain
language](https://www.iso.org/standard/78907.html) as a reader-focused guide.
The reader must be able to find, understand, and use the needed information.

This project does not claim formal compliance with every ASD-STE100
dictionary rule. New and changed prose must follow the ASD-STE100 writing
rules. Project terms such as `Grain`, `Activation`, and `CAS` are technical
terms. Define them before use.

Use American English spelling.

## Scope

This rule covers all new and changed prose in:

- `docs/`;
- `design/`;
- `README` files;
- `ROADMAP.md`;
- public API comments and examples.

Use English only. Code, names, commands, logs, URLs, and quoted source text
are not prose. Do not add Chinese translations next to the English text.

Use the terms in the root [CONTEXT.md](../CONTEXT.md). Add a new term only
when the current language cannot express the domain. Add the term to
`CONTEXT.md` when the project accepts it.

## Writing rules

- Put the subject and the verb near the start of a sentence.
- Use active voice. Say who does the action.
- Put one topic in each sentence.
- Use a common word when it has the needed meaning. Use `start`, not `begin`,
  `commence`, or `initiate`.
- Use one word for one meaning in one document.
- Use a technical noun or verb only when it is needed. Define it before use.
- Avoid long noun groups and abstract nouns when a direct verb works.
- Avoid idioms, jokes, slogans, marketing language, and vague claims.
- Avoid an `-ing` form when it can make the sentence unclear.
- Use `must` for a requirement, `may` for an option, and `must not` for a ban.
- State the user-visible result before the implementation detail.
- Use lists and tables when they make repeated facts easier to scan.
- Keep examples short, correct, and consistent with the public API.

Target sentence length is 20 words or fewer. Avoid sentences longer than 30
words unless the sentence is a code rule, a table cell, or a necessary exact
definition.

## Product and design documents

`docs/` uses product and domain language. It explains what a user can do and
what result the user can expect.

`design/` may use technical terms. It must still use plain sentences, and it
must define a term when a new reader may not know it.

Both layers must state failure behavior. Do not hide a retry rule, an
ordering rule, or a data-loss limit in an example.

## Review gate

Before merging a documentation change, check:

1. Is every prose sentence in English?
2. Can a new reader tell what to do and what will happen?
3. Is each special term needed and defined?
4. Are requirements and options written with `must`, `may`, and `must not`?
5. Are sentences short enough to read without parsing them twice?
6. Do examples and links point to real, current content?

Use a readability score as a warning, not as proof of clear writing. Aim for
Flesch-Kincaid Grade 9 or lower in `docs/` and Grade 11 or lower in `design/`.
When technical terms raise the score, shorten the nearby sentences and define
the terms.
