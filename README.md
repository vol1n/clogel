# Clogel

Clojure-native DSL and compiler for EdgeQL

## 🚀 Quick start

``` clojure
;; deps.edn
{:deps {vol1n/clogel {:git/url "https://github.com/vol1n/clogel.git"
                      :git/sha "275502bdcd7cbe49448542f4c8523e0d07bf8cc9"}}}
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

(g/query {:select {:User [:name
                          {:friends [:name]}]}})
                 
;; or

(g/query (g/select (g/User [:name 
                            {:friends [:name]}])))
                   
```

> Note: You should access clogel.core from an aliased or fully qualified namespace. clogel.core exposes a lot of functions from EdgeQL, including ones that overlap with clojure.core names.

## 💪 Features

- 🦺 Type-safe EdgeQL query builder
- 🧠 Schema-aware (with cardinality + link support)
- 🦄 Clojure-first: designed for REPL use
- 🔥 Supports insert, select, for/with, filtering, functions, slicing, and more
- 💥 No need to write EdgeQL strings — write real queries as data
- 📦 Optional `defquery` macro for defining and abstracting parameterized queries

## Usage

### Overview

Queries can be built using a somewhat simple EDN form, inspired by [Lacinia](https://github.com/walmartlabs/lacinia). 
> Example

``` clojure
(g/query (g/select {:User [:name                                          ;; simple field
                           {:friends [:name]}                             ;; expand ref field
                           {:= {:my_field (g/select "Hello world!")}}]})) ;; assignment field 
```

Optionally, you can use a function API that is macro-expanded depending on the schema of your currently active Gel instance. For instance,

``` clojure
(g/User [:name])
```

Projections are still in EDN.

### Functions and operators

Like with object forms, functions and operators can be accessed using raw EDN or macro-expanded functions. Function and operator calls are vectors in the EDN form:

``` clojure
[:+ "a" "b"]
```
or

``` clojure
(g/+ "a" "b")
```

> Note: array / JSON slicing `[x]` and `[x:y]` form are available from :access or (g/access)

### Top level forms

The idiomatic way to build queries at the top level is using the thread-first macro (`->`).

``` clojure
(->
    (g/select :User)
    (g/filter (g/= '.name "foo")))
```

As always, you can just use the EDN format if you'd like:

``` clojure
{:select :User
 :filter [:= '.name "foo"]}
```

### Composition

Because Clogel compiles to an intermediate representation then to EdgeQL when `g/query` or `defquery` are used, Clogel queries are fully composable. For example,

``` clojure
(def user-fields [:name :address :age])

(g/query (g/User user-fields))
```

This makes building queries dynamically from fragments super clean and simple, which is enabled by EdgeQL's composability.

### defquery

The `defquery` macro is probably the best way to work with Clogel. It compiles your query into a parameterized EdgeQL query at macro expansion time, allowing for quick feedback loops to ensure the query is syntactically valid before runtime. It then exposes your query as a Clojure function!

``` clojure
 (defquery 'sign-up!
     [['$email :str]
      ['$hashed :str]]
     (-> (g/insert :User [{:= {:email '$email}}
                          {:= {:password '$hashed}}])
         (g/unless-conflict '.email)))
                          
(let [new-user (sign-up! "colin@example.com" hashed-password)]
    (if (empty? new-user)
        (println "Email already in use")
        (println "Sign up successful! New User ID: "
                 (-> new-user first :id)))) ; EdgeQL queries return a set, we treat as a Clojure seq
```

> Sorry about the apostrophes. Custom clj-kondo linting is on my radar. 

## Limitations + Contributing

- Some constructs may not work as expected
- I will add features and fixes as they're needed in my personal use
- PRs welcome! I built this for my own personal tooling, but if you use it too, that's awesome! 

