#!/bin/bash

set -e

psql -c "SELECT Loader_Generate_Nation_Script('sh')" -d geocoder -tA > /gisdata/nation_script_load.sh
sed -i 's,PGUSER=postgres,PGUSER='"`cat /run/secrets/postgresql_user`"',g' /gisdata/nation_script_load.sh
sed -i 's,PGPASSWORD=yourpasswordhere,PGPASSWORD='"`cat /run/secrets/postgresql_password`"',g' /gisdata/nation_script_load.sh
sed -i 's,PGDATABASE=geocoder,PGDATABASE='"`cat /run/secrets/postgresql_db`"',g;' /gisdata/nation_script_load.sh
sed -i 's,${PSQL},${PSQL} -d ${PGDATABASE} -U ${PGUSER} -h ${PGHOST},g;' /gisdata/nation_script_load.sh
. /gisdata/nation_script_load.sh 