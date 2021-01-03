#!/bin/sh -ex
echo "$TRAVIS_COMMIT_MESSAGE" | head -n1 | grep -q '^MINOR:' && exit 0
if [ $DEPLOY ] ; then
    ./check-build-status.sh
    ./deploy.sh
    ./travis-downstream-trigger.sh
else
    ./codespeed-submit.sh
fi
