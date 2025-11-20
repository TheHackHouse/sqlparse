#
# Copyright (C) 2009-2020 the sqlparse authors and contributors
# <see AUTHORS file>
#
# This module is part of python-sqlparse and is released under
# the BSD License: https://opensource.org/licenses/BSD-3-Clause

from sqlparse import sql, tokens as T


class StatementSplitter:
    """Filter that split stream at individual statements"""

    def __init__(self):
        self._reset()

    def _reset(self):
        """Set the filter attributes to its default values"""
        self._in_declare = False
        self._in_case = False
        self._is_create = False
        self._begin_depth = 0

        self.consume_ws = False
        self.tokens = []
        self.level = 0
        self._skip_insert_into = False
        self._seen_insert = False

    def _maybe_skip_insert_into(self, ttype, value):
        """Flag INSERT ... INTO statements so we don't accumulate their tokens."""
        if self._skip_insert_into:
            return

        # Only pay attention to non-whitespace/comment tokens
        if ttype in T.Comment or ttype in T.Whitespace or ttype is T.Newline:
            return

        if not self._seen_insert and ttype is T.DML and value.upper() == "INSERT":
            self._seen_insert = True
            return

        # Second significant token after INSERT must be INTO to trigger skip
        if self._seen_insert:
            if ttype is T.Keyword and value.upper() == "INTO":
                self._skip_insert_into = True
                self.tokens.clear()
            else:
                # False alarm, reset the state
                self._seen_insert = False

    def _change_splitlevel(self, ttype, value):
        """Get the new split level (increase, decrease or remain equal)"""

        # parenthesis increase/decrease a level
        if ttype is T.Punctuation and value == '(':
            return 1
        elif ttype is T.Punctuation and value == ')':
            return -1
        elif ttype not in T.Keyword:  # if normal token return
            return 0

        # Everything after here is ttype = T.Keyword
        # Also to note, once entered an If statement you are done and basically
        # returning
        unified = value.upper()

        # three keywords begin with CREATE, but only one of them is DDL
        # DDL Create though can contain more words such as "or replace"
        if ttype is T.Keyword.DDL and unified.startswith('CREATE'):
            self._is_create = True
            return 0

        # can have nested declare inside of being...
        if unified == 'DECLARE' and self._is_create and self._begin_depth == 0:
            self._in_declare = True
            return 1

        if unified == 'BEGIN':
            self._begin_depth += 1
            if self._is_create:
                # FIXME(andi): This makes no sense.  ## this comment neither
                return 1
            return 0

        # BEGIN and CASE/WHEN both end with END
        if unified == 'END':
            if not self._in_case:
                self._begin_depth = max(0, self._begin_depth - 1)
            else:
                self._in_case = False
            return -1

        if (unified in ('IF', 'FOR', 'WHILE', 'CASE')
                and self._is_create and self._begin_depth > 0):
            if unified == 'CASE':
                self._in_case = True
            return 1

        if unified in ('END IF', 'END FOR', 'END WHILE'):
            return -1

        # Default
        return 0

    def process(self, stream):
        """Process the stream"""
        EOS_TTYPE = T.Whitespace, T.Comment.Single

        # Run over all stream tokens
        for ttype, value in stream:
            # Yield token if we finished a statement and there's no whitespaces
            # It will count newline token as a non whitespace. In this context
            # whitespace ignores newlines.
            # why don't multi line comments also count?
            if self.consume_ws and ttype not in EOS_TTYPE:
                if not self._skip_insert_into:
                    yield sql.Statement(self.tokens)

                # Reset filter and prepare to process next statement
                self._reset()

            # Identify INSERT ... INTO early so we can drop tokens immediately
            self._maybe_skip_insert_into(ttype, value)

            # Change current split level (increase, decrease or remain equal)
            self.level += self._change_splitlevel(ttype, value)

            # Append the token to the current statement
            if not self._skip_insert_into:
                self.tokens.append(sql.Token(ttype, value))

            # Check if we get the end of a statement
            # Issue762: Allow GO (or "GO 2") as statement splitter.
            # When implementing a language toggle, it's not only to add
            # keywords it's also to change some rules, like this splitting
            # rule.
            if (self.level <= 0 and ttype is T.Punctuation and value == ';') \
                    or (ttype is T.Keyword and value.split()[0] == 'GO'):
                self.consume_ws = True

        # Yield pending statement (if any)
        if self.tokens and not all(t.is_whitespace for t in self.tokens):
            yield sql.Statement(self.tokens)
