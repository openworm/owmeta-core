#!/bin/sh -ex

if [ $DEPLOY ] ; then
    sudo apt-get install ruby
    gem install travis
    exit 0
fi

TARGET=
if [ "$MYSQL_TEST" ] ; then
    TARGET=".[mysql_source_mysql_connector] .[mysql_source_mysqlclient]"
fi

if [ "$POSTGRES_TEST" ] ; then
    TARGET="$TARGET .[postgres_source_psycopg] .[postgres_source_pg8000]"
fi
TARGET="$TARGET .[sftp]"

pip install ${TARGET:-"."}
pip install -r test-requirements.txt
pip install --upgrade coveralls
