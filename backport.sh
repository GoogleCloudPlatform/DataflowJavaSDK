#!/bin/bash -ex

BEAM_PR=$1
MERGE_COMMIT=$2

LOCAL_BRANCH="backport-${BEAM_PR}"

git fetch --all
git rebase --onto dataflow/master "$MERGE_COMMIT"^1 "$MERGE_COMMIT"^2
git checkout -b "$LOCAL_BRANCH"
git push my-dataflow "$LOCAL_BRANCH"
