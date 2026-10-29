# Scala Pure-FP Style Guide

Canonical source: `pure-service-generator/conductor/code_styleguides/scala-fp.md` — copy
verbatim into each Scala project's `conductor/code_styleguides/`. Project-specific
examples live in that project's `development-guidelines.md`; this file states the rules.

Tagless final abstracts *which* effect runs the code. It does not decide *where* logic
lives. These rules cover both.

## 1. Functional core, effectful shell
- Business rules (state transitions, calculations, validation, decisions) are **pure
  functions** — no `F[_]`, no `IO`, no logger, no store. They take values (including an
  already-obtained `now`/id) and return values (`Either[DomainError, A]`, a new state,
  an event to emit).
- Effectful code (routes, consumers, stores, clients) only: obtains inputs (time, ids,
  rows, requests), calls the pure core, then executes its result (persist, publish, log,
  respond). It does not *decide* anything a pure function could.
- A rule is defined **once**. If the same rule appears in two store implementations,
  or in Scala and in SQL, extract it to the core and pass its output (e.g. an allowed
  `Set[Status]`) to each implementation. Atomic checks (compare-and-set in SQL) stay in
  the database, but their *parameters* come from the core.
- Keep pure domain code in its own package (e.g. `domain/`) containing no `F[_]`, so
  purity is checkable by location.

## 2. State machines
- Any entity with a lifecycle status has an explicit, pure transition function, e.g.
  `transition(current: Status, event: Event): Either[DomainError, Status]`, built from
  an exhaustive `match` on the event.
- Every path that changes status — HTTP, Kafka consumer, scheduled job — goes through it.
  No endpoint may set an arbitrary status unless the spec explicitly calls it an admin
  override.

## 3. Narrowest capability
- Constrain each function by the **least** powerful typeclass it needs:
  `Functor`/`Monad`/`MonadThrow` for sequencing, `Clock` for time, `Temporal` for
  sleeps/timeouts, `Concurrent` for `Ref`/`Deferred`, `Sync` for `delay`. `Async` only
  where actually required (FFI callbacks, network/Kafka streams, composition root).
- Prefer depending on an algebra (`OrderStore[F]`, `Logger[F]`) over a typeclass that
  grants arbitrary side effects. A constraint is documentation that the compiler proves.

## 4. Errors are values
- Expected, nameable failures are cases of a `sealed trait`/`enum` error ADT, carried in
  `Either`/`EitherT` from detection to the edge. Exceptions only for bugs and
  environment/startup failures.
- Don't collapse distinct outcomes into `Option`/`Boolean` when the caller needs to
  know *why* (e.g. not-found vs illegal-state) — return `Either[DomainError, A]`.
- Map each error case to an HTTP status explicitly at the edge (tapir `errorOut`); no
  catch-all exception translator.

## 5. Parse, don't validate
- Raw input (`String` ids, JSON fields) is parsed **once at the boundary** into domain
  types — opaque types / value classes for ids (`opaque type OrderId = UUID`), smart
  constructors returning `Either` for constrained values.
- Inner layers accept the parsed type, never re-parse. Repeated
  `Try(UUID.fromString(...))` is a smell that a domain type is missing.
- Use `Either.catchOnly[E]` over `Try(...).toOption` so only the expected exception is
  converted.

## 6. Totality
- No `.get` on `Option`/`Either`/`Try`, no `.head`/`.last`/`.reduce` on possibly empty
  collections (use `NonEmptyList`, `headOption`, `reduceOption`), no `asInstanceOf`.
- Pattern matches on sealed types are exhaustive with **no** `case _` — a new case must
  break compilation. `case _` is acceptable only over an open set (raw strings, unknown
  event types) and should log or route the unknown value, not silently swallow it.
- No `null`, no `var`, no mutable collections in domain or service code. State that
  changes over time lives in `Ref`/`Deferred`/the database.

## 7. Side effects captured in F
- Randomness, wall-clock time, id generation, blocking calls and side-effecting library
  calls happen only inside `F` (`Sync[F].delay`, `Clock[F]`, `UUIDGen[F]`, `Random[F]`)
  at the point of use. Never at object initialisation or in a `val`.
- Never call `unsafeRun*` outside `main`/test harnesses.
- Dependencies are explicit constructor/method parameters wired at the composition
  root — no annotations, reflection or macro DI.

## 8. Tests
- Pure core is tested with plain values (no `IO`), and lifecycle rules get a
  ScalaCheck property (e.g. "no event leaves a terminal state").
- Effectful shell is tested against in-memory algebra instances; integration tests use
  real dependencies (Testcontainers).

## Review checklist
Reviewers check each change against these; any "no" is a **High** finding.
- [ ] New business rules are pure functions outside `F`, defined once?
- [ ] Every status change goes through the entity's transition function?
- [ ] Each `F[_]` constraint is the narrowest that compiles?
- [ ] Failures typed as ADT cases, not `Option`/`Boolean`/exceptions, through to the edge?
- [ ] Raw input parsed to domain types at the boundary, not re-parsed inside?
- [ ] No partial functions, `null`, `var`, `asInstanceOf`; no `case _` over a sealed type?
- [ ] Time/randomness/ids/IO only inside `F`?
- [ ] Pure core covered by value-level tests (properties for state machines)?
