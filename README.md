# PostGIS Geocoder

This repo project aims to enable users to easily set up their own geocoding infrastructure using [US Census Bureau's geospatial TIGER/Line data](https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html). 

At present, it's provides functionality to:
* download geospatial data files for the US states you indicate in the `.env` file,
* create a PostgreSQL database as well as a user with the username and password you set in the `/secrets/postgresql_user.txt` and `/secrets/postgresql_password.txt` (respectively),
* load postgres extensions
    * postgis,
    * postgis_topology,
    * fuzzystrmatch,
    * postgis_tiger_geocoder,
    * address_standardizer, and
    * address_standardizer_data_us,
* create a pgadmin4 database administration server as well as a user with the username and password you set in the `/secrets/pgadmin_user.txt` and `/secrets/pgadmin_password.txt` (respectively), and
* ingest the downloaded data into the database.


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
    4. Repeat for the remaining 3 files.

3. Indicate the version (ie year) and set of US States/territories geometries you want to load into the database in the `.../postgis_geocoder/.env` file

    If you want the TIGER Geocoder to load geometries for all states from 2020, leave the default `.env` alone, but if you only want to load a handful of states or you want geometries from another year, edit the `.env` file. For example, if you only wanted to load midwestern states, reduce the listed state abbreviations (central midwestern sample shown below).

    ```bash
    GEOCODER_STATES=IA,IL,IN,MI,WI
    GEOCODER_YEAR=2020
    ```

4. Recommended step: Turn on a VPN and set a specific server location
    Loading a full set of data will involve downloading ~30GB of data from the US Census Bureau site. To limit the load on their servers, Census Bureau servers will only serve any given file to an IP address once per <some_time_period>, but if there's a network hiccup or any other issue, that can force you to have to wait that timespan before you can fill in any gaps in the downloaded set of TIGER files. Using a VPN (or otherwise changing your server's IP address) allows you to download missed files at will.

5. Download Census Data Files
    To download nationwide STATE and COUNTY shapefiles as well as PLACE, COSUB, TRACT, TABBLOCK, and BG shapefiles ((name definitions)[https://www2.census.gov/geo/tiger/TIGER2020/2020_TL_Shapefiles_File_Name_Definitions.pdf]) for the year and states listed in your `.env` file, execute the `download_tiger_data.sh` script as shown below. If you can't execute it, `chmod +x` the file and try again. Add the `-v` flag for verbose output.

        ```bash
        user@host:~/.../postgis_geocoder$ ./download_tiger_data.sh [-v]
        ```

    **Note: Downloading data for all states involves downloading 30GB+ and (in my experience) takes over 12 hours.**

    After the download script has finished, run the script again (it should finish nearly instantly as it won't re-download files if they're already downloaded) and scan through the output. If all lines indicate "All files ... successfully downloaded.", proceed to the next step. Otherwise, run the script again (with your VPN pointing to a different server if necessary).

    Second Note: As currently implemented, the data download shell script isn't run through the docker, but it might be in the future.

6. Build the images used in the docker-compose application
    Build the images for the docker-compose application via the command below. This step will temporarily double the disk usage of this project as it will copy this project's `context` (ie all of the files in this repo that aren't explicitly excluded in a `.dockerignore` file) to a temporary location that the docker daemon builds from before deleting those temporary copies. So if you include all states in your `.env` file, plan on having ~100GB available before proceeding.

        ```bash
        user@host:~/.../postgis_geocoder$ docker-compose build
        ```

    You'll have to repeat this step any time you change the `.env` file (or make any changes to the Dockerfiles or init_files).

7. Initialize the database and ingest data
    The following command starts up the postgis_geocoder docker-compose application. While starting, it will check for the `public_geocoder` volume (indicated in the `docker-compose.yml` file) and if it doesn't find that volume, it will create the volume and run through initialization steps that create the postgis database and ingest data earlier steps downloaded into the `/gisdata` directory.

        ```bash
        user@host:~/.../postgis_geocoder$ docker-compose up
        ```

    This step may take a bit and produce a lot of console output. When the data ingestion finishes, the console output will stop rapidly changing; scan through the last ~50 lines to see if the ingestion terminated from an error or if everything ingested smoothly. If you see an error, go to the troubleshooting section, otherwise, your postgis_geocoder server is up and running!


## ToDo:
Actually do some geocoding, then document it.



## Accessing pgadmin4

Go to 0.0.0.0:4327 in a browser and log in.

### Connecting a db

You can create a new server by right-clicking **Servers** (in the tray on the left edge of the screen) -> **Create** -> **Server...**.

In the interface that pops up, 
1. On the **General** tab: enter any name (this is what you will see in the pgadmin4 interface) 
1. On the **Connection** tab:
	1. **Host name/address**: enter the service name for the database from the `docker-compose.yml` file ("postgis")
	1. **Port**: Use the port number from inside the container (not the port number for the host machine)
	1. **Username**: enter the database user name from `/secrets/postgresql_user.txt`
    1. **Password**: enter the database password from `/secrets/postgresql_password.txt`

Then click save. If things work, you should be good to go.

## Accessing `psql` in a running container

The connection command will have the form
`\$ docker exec -ti NAME_OF_CONTAINER psql -U YOUR_POSTGRES_USERNAME NAME_OF_DB`

## Accessing an interactive shell in a running container

`\\$ docker exec -ti geocoder_postgis_cont bash`

# Troubleshooting

If your data ingestion finished early as a result of some error, look through the recent lines to see if it points to a specific data file that couldn't be ingested. If you see such a file, manually delete that file, escape docker-compose application via `ctrl+c` and execute the commands below to clear the volume, redownload data via the download script (and rerun that to confirm everything downloaded), rebuild your docker-compose application, and start it back up.

    ```bash
    user@host:~/.../postgis_geocoder$ docker-compose down -v
    user@host:~/.../postgis_geocoder$ ./download_tiger_data.sh
    user@host:~/.../postgis_geocoder$ ./download_tiger_data.sh
    user@host:~/.../postgis_geocoder$ docker-compose build
    user@host:~/.../postgis_geocoder$ docker-compose up
    ```

# Misc

Useful tip: if you want to capture console output to a file, tack on ` 2>&1 | tee build_logs_$(date +"%Y_%m_%d__%H_%M_%S").txt` after your command.

docker-compose build 2>&1 | tee build_logs_$(date +"%Y_%m_%d__%H_%M_%S").txt

2>&1 | tee download_logs_$(date +"%Y_%m_%d__%H_%M_%S").txt