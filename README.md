# PostGIS Geocoder

This project is mainly intended to be a turnkey geocoding service for addresses in the US (but it's also largely a learning project I'm using to develop my skill with Docker).

At present, it's configured to create a PostgreSQL database, load some extensions ()

# Setting up the system

1. Clone this repo and navigate into its top-level directory (`/postgis_geocoder`)

2. Set up secrets (eg passwords, usernames, and db_name) for your system
    1. create blank files in the correct location via

       ```bash
       user@host:~/.../postgis_geocoder$ mkdir secrets && \
       cd secrets && \
       touch postgresql_password.txt \
       postgresql_user.txt \
       postgresql_db.txt \
       pgadmin_password.txt \
       pgadmin_email.txt && \
       cd ..
       ```

    2. Open the file `.../postgis_geocoder/secrets/postgresql_password.txt` in a text editor, type in the string you want to use as the password for the postgres/postGIS database, save, and exit. Make sure there aren't any leading or trailing spaces (unless you want your password to have leading or trailing spaces).
    3. Open the file `.../postgis_geocoder/secrets/postgresql_user.txt` and type in the string you want to use as the username for the postgres/postGIS database.
    4. 5. 6. Repeat for the remaining 3 files.

3. Indicate the version (ie year) and set of US States/territories geometries you want to load into the database in the `.../postgis_geocoder/.env` file

    If you want the TIGER Geocoder to load geometries for all states from 2020, leave the default `.env` alone, but if you only want to load a handful of states or you want geometries from another year, edit the `.env` file. For example, if you only wanted to load midwestern states, reduce the listed state abbreviations (central midwestern sample shown below).

    ```bash
    GEOCODER_STATES=IA,IL,IN,MI,WI
    GEOCODER_YEAR=2020
    ```

4. Recommended step: Turn on a VPN and set a specific server location

Loading a full set of data can take over an hour, and if there's a network hiccup, it can short-circuit the rest of the loading of data. Additionally, as the full set of data is 10s of GB, the US Census site appears to have a limit on how frequently an IP address can download each file.

5. Attempt initialization
    5.1. Build the services used in the postgis_geocoder application

    ```bash
    user@host:~/.../postgis_geocoder$ docker-compose build
    ```

    5.2. Load TIGER data into the database
    This step will likely take hours (depending on the number of states indicated in your `.env` file)

    ```bash
    user@host:~/.../postgis_geocoder$ docker-compose --verbose up 2>&1 | tee compose_up_logs_02.txt
    ```

    5.3. When the console output stabilizes, check the logs to see if all data was loaded. Specifically, look for things like

        ```bash
        geocoder_postgis_cont | FINISHED --2022-03-27 19:45:15--
        geocoder_postgis_cont | Total wall clock time: 19s
        geocoder_postgis_cont | Downloaded: 1 files, 6.5M in 4.6s (1.41 MB/s)
        geocoder_postgis_cont | https://www2.census.gov/geo/tiger/TIGER2020/EDGES/tl_2020_16085_edges.zip:
        geocoder_postgis_cont | 2022-03-27 19:46:15 ERROR 500: Internal Server Error.
        ```

    near the bottom of the file. An error message is an obvious sign of an issue, and from the name of the last requested file (`tl_2020_16085_edges.zip`), we can see how much data was loaded. The process will load all of a state's feature-groups (eg [block, tract, county, edge, etc](https://www2.census.gov/geo/pdfs/maps-data/data/tiger/tgrshp2020/TGRSHP2020_TechDoc.pdf)) before iterating to the next state, and the state is indicated by the 2 digits after the `tl_YYYY_` characters. In this attempt, the run made it through much of state `16`, which, per this [reference](https://www2.census.gov/geo/docs/reference/state.txt), is Idaho.

    5.4 If the run failed, clear the volume via the below command, connect your VPN to a different server, and go back to step 5.1.
    NOTE: I need to find a better way to do this; ideally one that allows for resuming initialization where it failed rather than simply clearing the data volume and trying again until a run successfully completes without issue, but that's not yet implemented. In any case, it's a very good idea to capture logs during the initialization to help debug in the (likely) case that something causes the initial data load to short circuit.
    
        ```bash
        user@host:~/.../postgis_geocoder$ docker-compose down -v
        ```


## ToDo:
Actually do some geocoding, then document it.


```

## Accessing pgadmin4

Go to 0.0.0.0:5678 in a browser and log in.

### Connecting a db

You can create a new server by right-clicking **Servers** (in the tray on the left edge of the screen) -> **Create** -> **Server...**.

In the interface that pops up, 
1. On the **General** tab: enter any name (this is what you will see in the pgadmin4 interface) 
1. On the **Connection** tab:
	1. **Host name/address**: enter the service name for the database from the `docker-compose.yml` file
	1. **Port**: Use the port number from inside the container (not the port number for the host machine)
	1. **Username**: enter the database user name

Then click save. If things work, you should be good to go.

## Accessing `psql` in a running container

The connection command will have the form
`\$ docker exec -ti NAME_OF_CONTAINER psql -U YOUR_POSTGRES_USERNAME NAME_OF_DB`

## Accessing an interactive shell in a running container

`\\$ docker exec -ti geocoder_postgis_cont bash`