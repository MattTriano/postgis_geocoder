# PostGIS Geocoder

This project is mainly intended to be a turnkey geocoding service for addresses in the US (but it's also largely a learning project I'm using to develop my skill with Docker).

At present, it's configured to create a PostgreSQL database, load some extensions ()

# Starting up the system

After cloning and `cd`ing into this repo, make and `cd` into the `secrets` directory if it doesn't exist
`.../postgis_geocoder$ mkdir secrets && cd secrets`

Then create the secret-holding files required by the `docker-compose.yml` file via
```bash
touch pgadmin_email.txt pgadmin_password.txt postgresql_db.txt \
 postgresql_password.txt postgresql_user.txt
```
Did the database name (in `postgresql_db`) need to be secret? Probably not, but I was learning about docker secrets when I implemented that and I like a bit of security by obscurity.

Now that you've created those empty files, put only the string described by the filename in each file (eg if you want your postgres admin username to be "matt", put exactly 4 characters (specifically m, a, t, and t; but without the spaces, commas, and "and") in postgres_user.txt).

With your secrets set, `cd` back to the project root dir (via `cd ..`). 

If you want the TIGER Geocoder to load geometries for all states from 2020, leave the default `.env` alone, but if you only want to load a handful of states or you want geometries from another year, edit the `.env` file. For example, if you only wanted to load midwestern states, reduce the listed state abbreviations (central midwestern sample shown below).

```bash
GEOCODER_STATES=IA,IL,IN,MI,WI
GEOCODER_YEAR=2020
```

With your secrets and states set, you can build the geocoder image via `$ docker-compose build`, then you can start up the containers via `$ docker-compose up` (you may want to add `-d` at the end to detach the server and keep your terminal, but I kind of like seeing the output and when I want more, I tack on `--verbose` to see even more detail).

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