# Clogel

Clojure-native DSL and compiler for EdgeQL

## 🚀 Quick start

``` clojure
;; deps.edn
{:deps {vol1n/clogel {:git/url "https://github.com/vol1n/clogel.git"
                      :git/sha "<latest-sha>"}}}
```

## ✨ Why?

I wanted a Clojure-y way to write Gel queries, without strings

``` edgeql
select User {
  name, 
  friends: {
    name
  }
}
```

``` clojure
(require '[vol1n.clogel.core :as g])

{:select {:User [:name
                 {:friends [:name]}]}}
                 
;; or

(g/select (g/User [:name 
                   {:friends [:name]}]))
                   
```

## 💪 Features

- 🦺 Type-safe EdgeQL query builder
- 🧠 Schema-aware (with cardinality + link support)
- 🦄 Clojure-first: designed for REPL use
- 🔥 Supports insert, select, for/with, filtering, functions, slicing, and more
- 💥 No need to write EdgeQL strings — write real queries as data
- 📦 Optional `defquery` macro for defining and abstracting parameterized queries

## Limitations + Contributing

- Some constructs may not work as expected
- I will add features and fixes as they're needed in my personal use
- PRs welcome! I built this for my own personal tooling, but if you use it too, that's awesome! 

```

```
