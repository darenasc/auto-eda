# Auto-EDA

## MySQL
Runing the follwoing script it is possible to create a MySQL database for testing.

```docker run --name mysql-test -p 6603:3306 --rm -e MYSQL_USER= -e MYSQL_PASSWORD= -e MYSQL_ROOT_PASSWORD= -e MYSQL_DATABASE=metadata -d mysql:8.0```

You need to add preferred parameters for this environmental variables `MYSQL_USER`, `MYSQL_PASSWORD`, `MYSQL_ROOT_PASSWORD` when you run the script.  