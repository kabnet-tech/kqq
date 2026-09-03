#!/usr/bin/env bash
# Lock down kabnet-tech/kqq: branch protection, rulesets, and repo settings.
#
# GitHub blocks branch protection and rulesets on FREE-plan PRIVATE repos.
# Run this immediately after making the repo public (or upgrading the plan):
#
#   gh repo edit kabnet-tech/kqq --visibility public --accept-visibility-change-consequences
#   bash .github/lockdown.sh
#
# Idempotent: safe to re-run. Requires gh authed with admin on the repo.
set -euo pipefail

REPO="kabnet-tech/kqq"
BRANCH="main"

echo "==> Locking down ${REPO} (branch: ${BRANCH})"

# ── 1. Classic branch protection on main ────────────────────────────────────
# PRs required, CI required, admin included (admins cannot bypass), 1 approval.
echo "==> Applying classic branch protection to ${BRANCH}"
gh api "repos/${REPO}/branches/${BRANCH}/protection" -X PUT --input - <<'EOF'
{
  "required_status_checks": {
    "strict": true,
    "contexts": [
      "Test (ubuntu-latest)",
      "Test (macos-14)"
    ]
  },
  "enforce_admins": true,
  "required_pull_request_reviews": {
    "require_code_owner_reviews": false,
    "required_approving_review_count": 1,
    "dismiss_stale_reviews_on_push": true,
    "require_last_push_approval": true
  },
  "restrictions": null,
  "allow_force_pushes": false,
  "allow_deletions": false,
  "allow_skipped_pull_request_reviews": false,
  "block_creations": false,
  "lock_branch": false,
  "required_conversation_resolution": true,
  "required_linear_history": true
}
EOF
echo "    ✓ branch protection applied"

# ── 2. Ruleset: PR + CI required on main, nobody bypasses (incl. admins) ───
echo "==> Creating ruleset 'main-protection' (PR + CI, no bypass)"
RULESET_ID=$(gh api "repos/${REPO}/rulesets" -X POST --input - \
  --jq '.id' <<'EOF'
{
  "name": "main-protection",
  "target": "branch",
  "enforcement": "active",
  "conditions": {
    "ref_name": {
      "include": ["~DEFAULT_BRANCH"],
      "exclude": []
    }
  },
  "bypass_actors": [],
  "rules": [
    { "type": "deletion" },
    { "type": "non_fast_forward" },
    {
      "type": "pull_request",
      "parameters": {
        "required_approving_review_count": 1,
        "dismiss_stale_reviews_on_push": true,
        "require_code_owner_review": false,
        "require_last_push_approval": true,
        "required_review_thread_resolution": true,
        "allowed_merge_methods": ["merge", "squash", "rebase"]
      }
    },
    {
      "type": "required_status_checks",
      "parameters": {
        "strict_required_status_checks_policy": true,
        "required_status_checks": [
          { "context": "Test (ubuntu-latest)" },
          { "context": "Test (macos-14)" }
        ]
      }
    }
  ]
}
EOF
)
echo "    ✓ ruleset created (id: ${RULESET_ID})"

# ── 3. Ruleset: only admins may tag v* (releases) ──────────────────────────
echo "==> Creating ruleset 'release-tags-admin-only' for refs tags/v*"
gh api "repos/${REPO}/rulesets" -X POST --input - <<EOF
{
  "name": "release-tags-admin-only",
  "target": "push",
  "enforcement": "active",
  "conditions": {
    "ref_name": {
      "include": ["refs/tags/v*"],
      "exclude": []
    }
  },
  "bypass_actors": [
    { "actor_id": 5, "actor_type": "RepositoryRole", "bypass_mode": "always" }
  ],
  "rules": [
    { "type": "creation" },
    { "type": "deletion" },
    { "type": "non_fast_forward" }
  ]
}
EOF
echo "    ✓ tag ruleset created (only admins can create/delete v* tags)"

# ── 4. Repo settings ────────────────────────────────────────────────────────
echo "==> Applying repo settings"
gh api "repos/${REPO}" -X PATCH --input - > /dev/null <<'EOF'
{
  "delete_branch_on_merge": true,
  "allow_update_branch": true,
  "allow_squash_merge": true,
  "allow_merge_commit": true,
  "allow_rebase_merge": false,
  "web_commit_signoff_required": true
}
EOF
echo "    ✓ delete_branch_on_merge, signoff, merge settings applied"

# ── 5. Actions hardening ────────────────────────────────────────────────────
echo "==> Hardening Actions"
gh api "repos/${REPO}/actions/permissions" -X PUT -F enabled=true -F allowed_actions=selected > /dev/null
gh api "repos/${REPO}/actions/permissions/selected-actions" -X PUT \
  -F github_owned_allowed=true -F verified_allowed=true -F patterns_allowed='actions/checkout,actions/cache,actions/upload-artifact,actions/download-artifact,mlugg/setup-zig,softprops/action-gh-release' > /dev/null
echo "    ✓ allowed_actions=all → selected (pinned allowlist)"

# ── 6. Verify ───────────────────────────────────────────────────────────────
echo "==> Verification"
gh api "repos/${REPO}/branches/${BRANCH}/protection" --jq '"  protection: \(.required_status_checks.contexts | length) required checks, enforce_admins=\(.enforce_admins.enabled)"'
gh api "repos/${REPO}/rulesets" --jq '.[] | "  ruleset: \(.name) (\(.enforcement))"'

echo "==> Done. main is PR-only, CI-gated, admin-enforced; releases are admin-only."