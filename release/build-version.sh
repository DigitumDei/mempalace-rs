#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
default_repo_root="$(cd "$script_dir/.." && pwd)"
repo_root="${MEMPALACE_VERSION_REPO:-$default_repo_root}"
version_file="${MEMPALACE_VERSION_FILE:-release/version.toml}"
commit="${MEMPALACE_VERSION_COMMIT:-HEAD}"

die() {
    echo "error: $*" >&2
    exit 1
}

parse_series() {
    awk '
        {
            line = $0
            sub(/#.*/, "", line)

            if (line ~ /^[[:space:]]*major[[:space:]]*=/) {
                major_count += 1
                sub(/^[[:space:]]*major[[:space:]]*=[[:space:]]*/, "", line)
                sub(/[[:space:]]*$/, "", line)
                major = line
            } else if (line ~ /^[[:space:]]*minor[[:space:]]*=/) {
                minor_count += 1
                sub(/^[[:space:]]*minor[[:space:]]*=[[:space:]]*/, "", line)
                sub(/[[:space:]]*$/, "", line)
                minor = line
            }
        }
        END {
            valid = major_count == 1 && minor_count == 1
            valid = valid && major ~ /^(0|[1-9][0-9]*)$/
            valid = valid && minor ~ /^(0|[1-9][0-9]*)$/
            if (!valid) {
                exit 1
            }
            print major " " minor
        }
    '
}

series_at() {
    local revision="$1"
    git -C "$repo_root" show "$revision:$version_file" 2>/dev/null | parse_series
}

is_on_first_parent() {
    local revision="$1"
    git -C "$repo_root" rev-list --first-parent "$commit" \
        | awk -v target="$revision" '$0 == target { found = 1 } END { exit !found }'
}

git -C "$repo_root" rev-parse --verify "$commit^{commit}" >/dev/null 2>&1 \
    || die "version commit does not exist: $commit"

current_series="$(series_at "$commit")" \
    || die "$version_file must define exactly one non-negative major and minor value"
read -r major minor <<<"$current_series"

series_base=""
previous_series=""
while IFS= read -r candidate; do
    [ -n "$candidate" ] || continue
    candidate_series="$(series_at "$candidate" || true)"
    [ "$candidate_series" = "$current_series" ] || continue

    parent="$(git -C "$repo_root" rev-parse "${candidate}^1" 2>/dev/null || true)"
    parent_series=""
    if [ -n "$parent" ]; then
        parent_series="$(series_at "$parent" || true)"
    fi

    if [ "$parent_series" != "$current_series" ]; then
        series_base="$candidate"
        previous_series="$parent_series"
        break
    fi
done < <(
    git -C "$repo_root" log \
        --first-parent \
        -m \
        --format=%H \
        "$commit" \
        -- "$version_file"
)

[ -n "$series_base" ] \
    || die "could not locate the first mainline commit for release series $major.$minor"

introduced_series=false
if [ -z "$previous_series" ]; then
    introduced_series=true
else
    read -r previous_major previous_minor <<<"$previous_series"
    if ((
        10#$major < 10#$previous_major
        || (
            10#$major == 10#$previous_major
            && 10#$minor <= 10#$previous_minor
        )
    )); then
        die "release series must increase from $previous_major.$previous_minor to $major.$minor"
    fi
fi

best_current_patch=-1
best_current_commit=""
best_previous_patch=-1
best_previous_commit=""

while IFS= read -r tag; do
    [ -n "$tag" ] || continue
    if [[ ! "$tag" =~ ^v${major}\.${minor}\.(0|[1-9][0-9]*)$ ]]; then
        continue
    fi

    patch_text="${BASH_REMATCH[1]}"
    patch_value=$((10#$patch_text))
    tag_commit="$(git -C "$repo_root" rev-list -n 1 "$tag")"
    is_on_first_parent "$tag_commit" || continue

    if git -C "$repo_root" merge-base --is-ancestor "$series_base" "$tag_commit"; then
        if (( patch_value > best_current_patch )); then
            best_current_patch="$patch_value"
            best_current_commit="$tag_commit"
        fi
    elif $introduced_series \
        && git -C "$repo_root" merge-base --is-ancestor "$tag_commit" "$series_base"
    then
        if (( patch_value > best_previous_patch )); then
            best_previous_patch="$patch_value"
            best_previous_commit="$tag_commit"
        fi
    fi
done < <(
    git -C "$repo_root" tag \
        --merged "$commit" \
        --list "v${major}.${minor}.*"
)

if (( best_current_patch >= 0 )); then
    builds_since_stable="$(
        git -C "$repo_root" rev-list \
            --first-parent \
            --count \
            "${best_current_commit}..${commit}"
    )"
    patch=$((best_current_patch + builds_since_stable))
elif (( best_previous_patch >= 0 )); then
    builds_since_series_base="$(
        git -C "$repo_root" rev-list \
            --first-parent \
            --count \
            "${series_base}..${commit}"
    )"
    patch=$((best_previous_patch + 1 + builds_since_series_base))
else
    patch="$(
        git -C "$repo_root" rev-list \
            --first-parent \
            --count \
            "${series_base}..${commit}"
    )"
fi

printf '%s.%s.%s\n' "$major" "$minor" "$patch"
