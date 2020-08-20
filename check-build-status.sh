#!/bin/bash -e

LAST_CACHE=0
CACHE_INTERVAL=3
CACHED_TRAVIS_JOBS=$(mktemp travis-downstream-trigger.XXXXXXXXXX)

get_cached_travis_jobs () {
    NOW=$(date +"%s")
    if [ $(( NOW - LAST_CACHE )) -ge $CACHE_INTERVAL ] ; then
        travis show $TRAVIS_BUILD_NUMBER --no-interactive \
            | grep "^#$TRAVIS_BUILD_NUMBER" \
            | grep -v DEPLOY=1 > $CACHED_TRAVIS_JOBS
        LAST_CACHE=$(date +"%s")
    fi
    cat $CACHED_TRAVIS_JOBS
}

get_passed_jobs () {
    get_cached_travis_jobs | grep passed | wc -l
}

get_failed_jobs () {
    get_cached_travis_jobs | egrep 'fail|error|cancel' | wc -l
}

get_total_jobs () {
    get_cached_travis_jobs | wc -l
}

total_jobs=$(get_total_jobs)
passed_jobs=$(get_passed_jobs)
failed_jobs=$(get_failed_jobs)
while [ $failed_jobs -lt 1 -a $total_jobs -ne $passed_jobs ] ; do
    echo 'Waiting for other jobs to finish ...' >&2
    sleep 10
    echo . >&2
    passed_jobs=$(get_passed_jobs)
    failed_jobs=$(get_failed_jobs)
done

if [ $total_jobs -ne $passed_jobs ] ; then
    echo "One or more jobs failed. Not triggering downstream builds" >&2
    exit 1
fi
