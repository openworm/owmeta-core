#!/bin/bash -xe
HEAD_REV=$(git rev-parse HEAD)
ORIGIN_DEV_REV=$(git ls-remote origin refs/heads/develop | grep -E -o '^[^[:space:]]+')
if [ "$HEAD_REV" != "$ORIGIN_DEV_REV" ] ; then
    echo "Not deploying since we aren't on the 'develop' branch" >&2
    exit 0
fi

date=$(date +"%Y%m%d%H%M%S")
sed -i -r "s/__version__ = '([^']+)\\.dev0'/__version__ = '\\1.dev$date'/" owmeta_core/__init__.py
sed -i -r "s/^(.*)\\.dev0/\\1.dev$date/" version.txt
python setup.py egg_info sdist
twine upload -c "Built by travis-ci. Uploaded after $(date +"%Y-%m-%d %H:%M:%S")" dist/owmeta-core*tar.gz dist/owmeta_core*tar.gz
