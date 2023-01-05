#!/bin/bash
save_file_location=$1
options_table_name=$2

echo jys502943 | sudo -S sudo service mysql start

mysql -uroot -pJys502943! options_database --local_infile=1 <<EOF #opens mysql server and enters the commands written in EOF

SET GLOBAL local_infile=True;

TRUNCATE TABLE $options_table_name;

LOAD DATA LOCAL INFILE '$save_file_location' INTO TABLE $options_table_name FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROWS;

exit
EOF
