!!! WORK IN PROGRESS !!!

Testing:

1. Make sure Postgres >= 9.4 is installed and running locally with ident auth:

    $ psql <<< "select current_setting('server_version_num')"
     current_setting 
    -----------------
     90403
    (1 row)


2. Install required packages:

    $ npm install


3. Run tests:

    $ npm test
