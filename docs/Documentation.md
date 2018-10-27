# Auto-EDA

## MySQL
Runing the follwoing script it is possible to create a MySQL database for testing.

```docker run --name mysql-demo -p 6603:3306 --rm -e MYSQL_USER=demo -e MYSQL_PASSWORD=demo -e MYSQL_ROOT_PASSWORD=rootpassword -e MYSQL_DATABASE=metadata -d mysql:8.0```

You need to add preferred parameters for this environmental variables `MYSQL_USER`, `MYSQL_PASSWORD`, `MYSQL_ROOT_PASSWORD` when you run the script. 

```
docker cp world.sql mysql-demo:/tmp
docker exec -it mysql-demo bash
mysql -prootpassword < /tmp/world.sql
```

You need to setup a connection to the source database and one connection to the meteadata database. In this example both databases are in the MySQL:
```
HOST_SORUCE = 'ip_of_the_mysql_host'
SCHEMA_SORUCE = 'world'
USER_SORUCE = 'root'
PASSWORD_SORUCE = 'rootpassword'
PORT_SORUCE = 6603

HOST_METADATA = 'ip_of_the_mysql_host'
SCHEMA_METADATA = 'demo'
USER_METADATA = 'demo'
PASSWORD_METADATA = 'metadata'
PORT_METADATA = 6603
```
Add your own credentials to the code and run the following function to get the metadata from the source database.

    describe_database('your_server', 'def', 'world')