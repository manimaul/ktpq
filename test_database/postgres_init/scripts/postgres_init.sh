#!/usr/bin/env sh

set -e

until psql -c '\q'; do
  >&2 echo "Postgres is unavailable - sleeping"
  sleep 1
done

>&2 echo "Postgres is up - executing command"


if [ "$(psql postgres -tXAc "SELECT 1 FROM pg_database WHERE datname='admin';")" != 1 ]
then
  echo "creating postgres database admin"
  createdb admin
  echo "test database setup complete!!!"
fi
