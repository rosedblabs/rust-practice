# expr-eval

An arithmetic expression parser and evaluator based on **operator-precedence parsing** — an elegant, compact algorithm that uses operator precedence and associativity to resolve order of operations (including parentheses) without building a separate AST.

The whole implementation is ~200 lines, which makes it a nice hands-on exercise. Through this project you can learn:

* An elegant, compact expression-evaluation algorithm (handy for "write a calculator" interview questions)
* Rust's basic data types, enums, and structs
* Functions and recursion
* `match` expressions
* Custom `Result`-based error handling
* Common iterator usage: `next`, `peekable`, etc.
* Writing a custom iterator
* `Option` usage

**How it works, in short:** a `Tokenizer` turns the source string into a stream of `Token`s (numbers, operators, parentheses) and implements `Iterator`, so it can be consumed with a `for` loop or wrapped in `Peekable`. `Expr::compute_expr` then evaluates left to right, recursing into a sub-expression only when the next operator's precedence is at least the current minimum (`min_prec`); right-associative operators like `^` keep the same `min_prec` on recursion, while left-associative ones bump it by one — that's the whole trick.

Further reading (Chinese): [太优雅了！Rust 200 行代码实现表达式解析](https://mp.weixin.qq.com/s/MuuaROoH7gI0wYVypEOoWw)
