#!/bin/sh -ex

if [ $DEPLOY ] ; then
    sudo apt-get install ruby
    gem install travis
    exit 0
fi

EXTRAS=
if [ "$MYSQL_TEST" ] ; then
    EXTRAS="mysql_source_mysql_connector,mysql_source_mysqlclient"
fi

if [ "$POSTGRES_TEST" ] ; then
    EXTRAS="${EXTRAS:+$EXTRAS,}postgres_source_psycopg,postgres_source_pg8000"
fi
EXTRAS="${EXTRAS:+$EXTRAS,}sftp"

pip install -e ".[${EXTRAS}]"
pip install -r test-requirements.txt
pip install --upgrade coveralls
