#!/usr/bin/env bash
# Wire-compat gate for the public protos.
#
# Compiles the turbovec/v1 protos at HEAD and at a base ref (default
# origin/main) into descriptor sets and runs `buf breaking` over them.
# Wire-breaking changes fail the build. An intentional pre-1.0 break is made
# by putting `[proto-breaking]` in the HEAD commit message (or exporting
# PROTO_BREAKING_OK=1), which turns the failure into a printed report.
#
# Requires: protoc, buf, git. See docs/scaling.md §7 for the policy.
set -euo pipefail

BASE_REF="${1:-origin/main}"
ROOT=$(git rev-parse --show-toplevel)
cd "$ROOT"

# The public wire surface. Add files here, never subtract silently: a file
# deleted from proto/ but left in this list fails the new-tree compile.
PROTOS=(turbovec/v1/turbovec.proto turbovec/v1/coordinator.proto)

new_dir=$(mktemp -d)
old_dir=$(mktemp -d)
trap 'rm -rf "$new_dir" "$old_dir"' EXIT

# Whole proto/ trees at both refs, so imports of files outside the listed
# surface (e.g. a since-deleted schema.proto) still resolve at compile time.
cp -r proto/. "$new_dir/"
git archive "$BASE_REF" proto | tar -x --strip-components=1 -C "$old_dir"

new_list=()
old_list=()
for p in "${PROTOS[@]}"; do
  [[ -f "$new_dir/$p" ]] && new_list+=("$p")
  [[ -f "$old_dir/$p" ]] && old_list+=("$p")
done

[[ ${#new_list[@]} -gt 0 ]] || { echo "no protos found at HEAD"; exit 1; }
[[ ${#old_list[@]} -gt 0 ]] || { echo "no protos found at $BASE_REF"; exit 1; }

protoc -I "$new_dir" --descriptor_set_out="$new_dir/set.binpb" --include_imports "${new_list[@]}"
protoc -I "$old_dir" --descriptor_set_out="$old_dir/set.binpb" --include_imports "${old_list[@]}"

allow_break=0
if [[ "${PROTO_BREAKING_OK:-0}" == "1" ]] ||
  git log -1 --format=%B HEAD | grep -qF '[proto-breaking]'; then
  allow_break=1
fi

if buf breaking "$new_dir/set.binpb" --against "$old_dir/set.binpb"; then
  echo "proto-compat: HEAD is wire-compatible with $BASE_REF"
else
  if [[ $allow_break -eq 1 ]]; then
    echo "proto-compat: breaking changes REPORTED but allowed (intentional break)"
    exit 0
  fi
  cat >&2 <<'EOF'
proto-compat: wire-breaking change against the base ref.
Additive-only by default; reserve removed tags and names (docs/scaling.md §7).
If this break is intentional, put [proto-breaking] in the commit message.
EOF
  exit 1
fi
