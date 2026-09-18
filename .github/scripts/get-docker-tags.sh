#!/usr/bin/env bash

# get-docker-tags.sh produces Docker tags for the current build
#
# Usage:
#   ./get-docker-tags.sh <build number> <git commit sha1> <git branch name> [git tag name]
#
# Example:
#
#   # get tag for the main branch
#   ./get-docker-tags.sh $(date -u +%F) testingsha main
#
#   # get tag for a release tag
#   ./get-docker-tags.sh $(date -u +%F) testingsha release v0.5.0
#
#   # Serving suggestion in CI
#   ./get-docker-tags.sh $(date -u +%F) "$CI_SHA1" "$CI_BRANCH" "$CI_TAG"
#
set -euo pipefail

if [[ $# -lt 1 ]] ; then
  echo 'At least 1 arg required.'
  echo 'Usage:'
  echo './get-docker-tags.sh <build number> [git commit sha1] [git branch name] [git tag name]'
  exit 1
fi

BUILD_NUM=$1
GIT_SHA1=${2:-$(git rev-parse HEAD)}
GIT_SHA1_SHORT=$(echo "$GIT_SHA1" | cut -c 1-7)
GIT_BRANCH=${3:-$(git symbolic-ref -q --short HEAD || echo "unknown")}
GIT_TAG=${4:-$(git describe --tags --exact-match 2> /dev/null || echo "")}

IMAGE_NAME=${IMAGE_NAME:-ipni/needle}

echoImageName () {
  local IMAGE_TAG=$1
  echo "$IMAGE_NAME:$IMAGE_TAG"
}

if [[ $GIT_TAG =~ ^v[0-9]+\.[0-9]+\.[0-9]+-rc ]]; then
  echoImageName "$GIT_TAG"

# This fork's own releases, vX.Y.Z-ipni.N, where vX.Y.Z is the upstream release
# the fork sits on. Without this they fall through to the else below and the
# build fails, because the "Nothing to do" line becomes the tags: input to
# docker/build-push-action.
#
# Deliberately no "latest": in semver everything after the hyphen is a
# prerelease, so vX.Y.Z-ipni.N sorts BEFORE vX.Y.Z, and pointing latest at it
# would advertise the fork build as older than the upstream release it is
# actually ahead of. Consumers pin the exact tag or a digest.
elif [[ $GIT_TAG =~ ^v[0-9]+\.[0-9]+\.[0-9]+-ipni\.[0-9]+$ ]]; then
  echoImageName "$GIT_TAG"

elif [[ $GIT_TAG =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  echoImageName "$GIT_TAG"
  echoImageName "latest"

elif [ "$GIT_BRANCH" = "main" ] || [ "$GIT_BRANCH" = "staging" ]; then
  echoImageName "${GIT_BRANCH}-${BUILD_NUM}-${GIT_SHA1_SHORT}"
  echoImageName "${GIT_BRANCH}-latest"

else
  echo "Nothing to do. No docker tag defined for branch: $GIT_BRANCH, tag: $GIT_TAG"

fi
