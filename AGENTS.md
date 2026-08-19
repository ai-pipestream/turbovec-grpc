# AGENTS.md

## Development line

- `main` is the development line. The `thin-coordinator` branch was merged into
  it (PR #1) and the transitional `Documents` service was subsequently removed;
  git history preserves that implementation as migration material for
  `turbovec-search`.

## Remotes and push policy

- **GitHub-only for now** (`origin` → `github.com/ai-pipestream/turbovec-grpc`).
  No Forgejo repo exists yet. The ai-pipestream rule is forgejo-first,
  github-second — so when a Forgejo repo is created for this project, push
  there first and GitHub second from then on.
- Workspace-wide policy and the per-repo remote table live in the
  workspace-root `../AGENTS.md` — read it before pushing anywhere.
