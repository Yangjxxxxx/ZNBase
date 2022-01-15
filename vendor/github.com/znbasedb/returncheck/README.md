# returncheck

This is forked from https://github.com/kkaneda/returncheck to make two main changes:
  1) de-`internal` implementation
	2) remove the hardcoded invocation against the since-renamed znbasedb package.

These changes were made since ZNBaseDB's linters are now Go tests, so we're happy to just directly invoke the Go function the main method was wrapping.
