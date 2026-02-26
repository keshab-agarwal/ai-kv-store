# Coding Style Guidelines:
- Minimize tight coupling between different files by coming up with the simplest and necessary APIs only
- Write simple, readable code with minimal indirection
- Avoid unnecessary object attributes and local variables
- No redundant abstractions or duplicate code
- Each function should do one thing well
- Use clear, descriptive names
- NO need to write documentations or comments unless absolutely necessary
- NO bloat or over-engineering of the system

# Testing Guidelines
- Run lint and type checkers and fix any lint and typecheck errors
- Generate comprehensive tests so that you achieve 100% branch coverage
- Tests MUST NOT use mocks, patches, or any form of test doubles
- Integration tests are highly encouraged
- You MUST not add tests that are redundant or duplicate of existing tests or does not add new - coverage over existing tests
- Generate meaningful stress tests for the code if you are optimizing the code for performance
- Each test should be independent and verify actual behavior
- Simplify and clean up the test code
Play Devil's advocate when you test--try to break the system in all possible ways.

# Editing and Bug Fixing Instructions
- For any change or bug fix, pause and think what would be the simple, elegant, and minimal changes.
- Find root causes of a bug and fix it, rather that applying a hacky, temporary fix.
- Do not over-engineer the fixes

# Post Coding Instructions
- After you have implemented a task, aggressively and carefully simplify and clean up the code
- Remove unnecessary object/struct attributes, variables, config variables
- Avoid object/struct attribute redirections
- Remove unnecessary conditional checks
- Remove redundant and duplicate code
- Remove unnecessary comments except for function and method documentations
- Make sure that the code is still working correctly
