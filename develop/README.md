## Quick Start


Prerequisites:
- Install Docker Engine and Docker Compose
Ensure Docker Engine and Docker Compose are installed and configured.
- Ensure Pixels is installed and configured in your local or remote Maven repository.


```bash
./install # or your relative/full path
```

After execute install script, you can check [Kafdrop](http://localhost:9000) to see if installation was successful.

Use this script to clean up

```bash
. scripts/common_func.sh
shutdown_containers
```

## Test


```bash
docker exec -it pixels_mysql_source_db mysql -upixels -ppixels_realtime_crud -D pixels_realtime_crud

docker exec -it pixels_postgres_source_db psql -Upixels -d pixels_realtime_crud
```