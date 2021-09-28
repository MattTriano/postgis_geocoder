#!/bin/bash

set -e

# Perform all actions as $POSTGRES_USER
export PGUSER="$POSTGRES_USER"

# Create the 'template_postgis' template db
"${psql[@]}" <<- 'EOSQL'
CREATE DATABASE template_postgis IS_TEMPLATE true;
EOSQL

# Load PostGIS into both template_database and $POSTGRES_DB
for DB in template_postgis "$POSTGRES_DB"; do
	echo "Loading PostGIS extensions into $DB"
	"${psql[@]}" --dbname="$DB" <<-'EOSQL'
		CREATE EXTENSION IF NOT EXISTS postgis;
		CREATE EXTENSION IF NOT EXISTS postgis_topology;
		CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;
		CREATE EXTENSION IF NOT EXISTS postgis_tiger_geocoder;
		CREATE EXTENSION IF NOT EXISTS address_standardizer;
		CREATE EXTENSION IF NOT EXISTS address_standardizer_data_us;

EOSQL
done

psql -c "SELECT Loader_Generate_Nation_Script('sh')" -d geocoder -tA > /gisdata/nation_script_load.sh
sed -i 's,PGUSER=postgres,PGUSER='"`cat /run/secrets/postgresql_user`"',g' /gisdata/nation_script_load.sh
sed -i 's,PGPASSWORD=yourpasswordhere,PGPASSWORD='"`cat /run/secrets/postgresql_password`"',g' /gisdata/nation_script_load.sh
sed -i 's,PGDATABASE=geocoder,PGDATABASE='"`cat /run/secrets/postgresql_db`"',g;' /gisdata/nation_script_load.sh
sed -i 's,${PSQL},${PSQL} -d ${PGDATABASE} -U ${PGUSER} -h ${PGHOST},g;' /gisdata/nation_script_load.sh
# sh /gisdata/nation_script_load.sh 