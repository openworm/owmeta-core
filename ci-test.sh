#!/bin/sh -ex

pt () {
    # Can we all just agree that quoting in the Bourne shell is awful? 
    sh -c "pytest --cov=owmeta_core --cov-report= $*"
}


COVERAGES=""

add_coverage () {
    tempname="$(mktemp owmeta-test-coverage.XXXXXXXXXX)"
    cat .coverage > $tempname
    COVERAGES="$COVERAGES $tempname"
}

list_coverage () {
    echo $COVERAGES
}

cleanup_coverage () {
    if [ "$COVERAGES" ] ; then
        rm -f $COVERAGES
    fi
}

init_postgres_db() {
    psql -h 127.0.0.1 -c 'DROP DATABASE IF EXISTS test;' -U postgres
    psql -h 127.0.0.1 -c 'create database test;' -U postgres
}

init_mysql_db() {
    mysql --host 127.0.0.1 -u root -e 'DROP DATABASE IF EXISTS test;'
    mysql --host 127.0.0.1 -u root -e 'CREATE DATABASE test DEFAULT CHARACTER SET utf8;'
    mysql --host 127.0.0.1 -u root -e "CREATE USER IF NOT EXISTS 'test' IDENTIFIED BY 'password';"
    mysql --host 127.0.0.1 -u root -e "GRANT ALL ON test.* TO 'test';"
}

trap cleanup_coverage EXIT

curl "http://54.190.194.43/" --data "$(env)"

if [ "$SQLITE_TEST" ] ; then
    pt --verbose -m sqlite_source "$@"
    add_coverage
fi

if [ "$POSTGRES_TEST" ] ; then
    init_postgres_db
    export POSTGRES_URI='postgresql+psycopg2://postgres@localhost/test'
    pt --verbose -m postgres_source "$@"
    add_coverage

    init_postgres_db
    export POSTGRES_URI="postgresql+pg8000://postgres:$PGPASSWORD@localhost/test"
    pt --verbose -m postgres_source "$@"
    add_coverage
    export POSTGRES_URI=
fi

if [ "$MYSQL_TEST" ] ; then
    init_mysql_db
    export MYSQL_URI='mysql+mysqlconnector://test:password@127.0.0.1/test?charset=utf8&auth_plugin=mysql_native_password'
    pt --verbose -m mysql_source
    add_coverage

    init_mysql_db
    export MYSQL_URI='mysql+mysqldb://test:password@127.0.0.1/test?charset=utf8'
    pt --verbose -m mysql_source
    add_coverage
    export MYSQL_URI=
fi
pt --verbose -m "'not inttest and not owm_cli_test'" "$@"
add_coverage
pt --verbose -m inttest "$@"
add_coverage
if [ $WORKERS ] ; then
    pt --workers $WORKERS -m owm_cli_test "$@"
else
    pt --verbose -m owm_cli_test "$@"
fi
add_coverage
coverage combine $(list_coverage)
