# Repository instructions for coding agents

Goal: develop an idiomatic, reliable Perl Discord API SDK intended for CPAN.

Read `CONTRIBUTING.md`, `docs/DESIGN.md` and the POD in `lib/Discord/Client.pm`
before making changes.

Operating rules:
- Prioritize readability in all programs, including samples and tests. Follow
  the Readability section of `CONTRIBUTING.md` and comment each logical
  processing group in English.
- Keep dependencies focused. Runtime stack is IO::Async, Future, Net::Async::HTTP and Net::Async::WebSocket::Client. Do not introduce Mojo, a second event loop, or a large object framework by default.
- Keep REST, Gateway protocol, Gateway transport and commands logically independent.
- Never invent Discord API behavior. Verify protocol changes with the current official Discord docs.
- Any change to a protocol edge case needs deterministic regression tests. Mock I/O in normal CI; live Discord smoke tests must be opt-in.
- Use Test2::V1 and its `T2` handle for tests. Keep test syntax compatible with Perl 5.26.
- Public async methods return Future. Do not force Future::AsyncAwait on consumers.
- Do not auto-sync application commands on startup, retry ambiguous POSTs by default, or log secrets.
- No CPAN upload or GitHub publishing without explicit permission.
- Before completion run `prove -lr t`, report failures, and identify tests blocked by unavailable dependencies or credentials.
- Keep changes focused; update relevant docs and behavior tests with code changes.
- Record only published releases in `Changes`; do not add development notes or
  unreleased change lists. The first release entry only needs "Initial release".
- Keep public API documentation in module POD. Check edited POD with `podchecker`.
- Place reference links in the explanation they support; do not append reference lists.

Current implementation:
- Public methods and usage are documented in each module's POD under
  `lib/Discord/Client.pm` and `lib/Discord/Client/`.
- `Gateway::Session`, the WebSocket adapter and the Identify and REST schedulers
  remain internal.
- `docs/DESIGN.md` describes the architecture for maintainers.
- Keep normal tests offline and deterministic. Test input, observable behavior
  and failure conditions; avoid asserting private implementation structure.
