# Git Workflow

All changes to this repository go through a pull request. Nothing is committed directly to `main`.

---

## The rule

> **No direct pushes to `main`.  
> Every change = a branch + a PR + an approval + a merge.**

This is enforced by GitHub branch protection. Attempting to push directly to `main` will be rejected.

---

## Day-to-day workflow

```bash
# 1. Start from an up-to-date main
git checkout main
git pull origin main

# 2. Create a branch named after what you are doing
git checkout -b <type>/<short-description>
# Examples:
#   feature/phase6-alignment-report
#   fix/spec-log-dedup-bug
#   docs/update-phase5-data-requisite
#   chore/bump-anthropic-sdk

# 3. Make your changes, commit in logical units
git add <files>
git commit -m "concise present-tense description"

# 4. Push the branch
git push -u origin <branch-name>

# 5. Open a pull request
gh pr create --title "..." --body "..."
# Or open in browser: gh pr create --web

# 6. Get the PR approved, then merge
gh pr merge --squash   # squash into a single clean commit on main
```

---

## Branch naming

| Type | Pattern | Example |
|------|---------|---------|
| New feature | `feature/<description>` | `feature/phase6-alignment-report` |
| Bug fix | `fix/<description>` | `fix/spec-log-index-dedup` |
| Documentation | `docs/<description>` | `docs/architecture-narrative` |
| Refactor | `refactor/<description>` | `refactor/assembler-column-filter` |
| Chore (deps, config) | `chore/<description>` | `chore/update-anthropic-sdk` |

---

## Commit messages

- Present tense, imperative: `add DataRequisite to SpecDocument` not `added` or `adds`
- First line ≤ 72 characters
- No period at the end
- If the reason is non-obvious, add a blank line and a short body paragraph

---

## Pull requests

A PR should be:
- **Focused** — one logical change per PR, not a bundle of unrelated edits
- **Self-contained** — tests passing, no broken imports
- **Described** — title and body explain what changed and why (the PR template prompts this)

The PR template (`.github/pull_request_template.md`) appears automatically when you open a PR on GitHub.

---

## Merging

- **Squash merge** is the default — one commit per PR on `main`, clean history
- Delete the branch after merging (`gh pr merge --squash --delete-branch`)
- Do not merge your own PR until it has at least one approval

---

## Branch protection summary

These rules are enforced at the GitHub level on `main`:

| Rule | Setting |
|------|---------|
| Require pull request before merging | Enabled |
| Required approving reviews | 1 |
| Dismiss stale reviews on new push | Enabled |
| Allow direct pushes | Disabled |
| Allow force pushes | Disabled |

---

## Solo / collaborator note

If you are working solo and need to merge your own PRs, you have two options:

1. Set required approvals to 0 (PRs still required, just not blocked on approval)
2. Add a collaborator who can approve

To change the required approval count:
```bash
gh api repos/christianvanbellen/enterprise-data-product-planner/branches/main/protection \
  --method PUT \
  --field required_status_checks=null \
  --field enforce_admins=false \
  --field "required_pull_request_reviews[required_approving_review_count]=0" \
  --field "required_pull_request_reviews[dismiss_stale_reviews]=true" \
  --field restrictions=null
```
