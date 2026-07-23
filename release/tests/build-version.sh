#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
version_script="$repo_root/release/build-version.sh"
temporary_root="$(mktemp -d)"
cleanup() {
    rm -rf "$temporary_root"
}
trap cleanup EXIT

new_repo() {
    local name="$1"
    local directory="$temporary_root/$name"
    mkdir -p "$directory"
    git -C "$directory" init -q -b main
    git -C "$directory" config user.name "Release Version Test"
    git -C "$directory" config user.email "release-version-test@example.invalid"
    printf '%s\n' "$directory"
}

write_series() {
    local directory="$1"
    local major="$2"
    local minor="$3"
    mkdir -p "$directory/release"
    printf 'major = %s\nminor = %s\n' "$major" "$minor" \
        > "$directory/release/version.toml"
}

commit_all() {
    local directory="$1"
    local message="$2"
    git -C "$directory" add .
    git -C "$directory" commit -q -m "$message"
}

empty_commit() {
    local directory="$1"
    local message="$2"
    git -C "$directory" commit -q --allow-empty -m "$message"
}

build_version() {
    local directory="$1"
    MEMPALACE_VERSION_REPO="$directory" bash "$version_script"
}

expect_version() {
    local directory="$1"
    local expected="$2"
    local actual
    actual="$(build_version "$directory")"
    [ "$actual" = "$expected" ] \
        || {
            echo "expected version $expected, got $actual" >&2
            exit 1
        }
}

expect_failure() {
    local directory="$1"
    if build_version "$directory" >"$temporary_root/expected-failure.log" 2>&1; then
        echo "expected version calculation to fail for $directory" >&2
        exit 1
    fi
}

linear_repo="$(new_repo linear)"
write_series "$linear_repo" 1 4
commit_all "$linear_repo" "start 1.4"
expect_version "$linear_repo" 1.4.0

empty_commit "$linear_repo" "first build"
first_build_commit="$(git -C "$linear_repo" rev-parse HEAD)"
expect_version "$linear_repo" 1.4.1
[ "$(
    MEMPALACE_VERSION_REPO="$linear_repo" \
        MEMPALACE_VERSION_COMMIT="$first_build_commit" \
        bash "$version_script"
)" = 1.4.1 ] || {
    echo "rerunning the same commit changed its version" >&2
    exit 1
}

printf '# release operators own major and minor\n' \
    >> "$linear_repo/release/version.toml"
commit_all "$linear_repo" "document version file"
expect_version "$linear_repo" 1.4.2

write_series "$linear_repo" 1 5
commit_all "$linear_repo" "start 1.5"
expect_version "$linear_repo" 1.5.0
empty_commit "$linear_repo" "next 1.5 build"
expect_version "$linear_repo" 1.5.1

write_series "$linear_repo" 2 0
commit_all "$linear_repo" "start 2.0"
expect_version "$linear_repo" 2.0.0

stable_repo="$(new_repo stable)"
write_series "$stable_repo" 3 2
commit_all "$stable_repo" "start 3.2"
git -C "$stable_repo" tag v3.2.0
empty_commit "$stable_repo" "3.2.1 build"
expect_version "$stable_repo" 3.2.1
git -C "$stable_repo" tag v3.2.1
empty_commit "$stable_repo" "3.2.2 build"
expect_version "$stable_repo" 3.2.2

migration_repo="$(new_repo migration)"
printf 'legacy release\n' > "$migration_repo/README.md"
commit_all "$migration_repo" "stable 0.1.0"
git -C "$migration_repo" tag v0.1.0
empty_commit "$migration_repo" "legacy unversioned candidate"
write_series "$migration_repo" 0 1
commit_all "$migration_repo" "introduce automatic versioning"
expect_version "$migration_repo" 0.1.1

merge_repo="$(new_repo merge)"
write_series "$merge_repo" 1 0
commit_all "$merge_repo" "start 1.0"
git -C "$merge_repo" switch -q -c feature
write_series "$merge_repo" 1 1
commit_all "$merge_repo" "start 1.1 on feature"
git -C "$merge_repo" switch -q main
empty_commit "$merge_repo" "mainline build"
git -C "$merge_repo" merge -q --no-ff feature -m "merge 1.1"
expect_version "$merge_repo" 1.1.0

decrease_repo="$(new_repo decrease)"
write_series "$decrease_repo" 2 0
commit_all "$decrease_repo" "start 2.0"
write_series "$decrease_repo" 1 9
commit_all "$decrease_repo" "invalid decrease"
expect_failure "$decrease_repo"

invalid_repo="$(new_repo invalid)"
mkdir -p "$invalid_repo/release"
printf 'major = 1\nmajor = 2\nminor = 0\n' \
    > "$invalid_repo/release/version.toml"
commit_all "$invalid_repo" "invalid duplicate major"
expect_failure "$invalid_repo"

echo "build version tests passed"
