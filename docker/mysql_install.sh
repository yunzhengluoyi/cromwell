#!/bin/bash

export DEBIAN_FRONTEND=noninteractive
apt-get install -y mysql-server mysql-client
service mysql start
mysqladmin create cromwell

mkdir ~/liquibase
pushd ~/liquibase
wget https://github.com/liquibase/liquibase/releases/download/liquibase-parent-3.3.5/liquibase-3.3.5-bin.tar.gz
tar -xvzf liquibase-3.3.5-bin.tar.gz
popd
rm -rf ~/liquibase

./liquibase --driver=com.mysql.jdbc.Driver \
  --classpath=$(find ~/.ivy2 | grep "jars/mysql-connector-java-.*jar") \
  --changeLogFile=/cromwell/src/main/migrations/changelog.xml \
  --url="jdbc:mysql://localhost/cromwell" \
  --username="root" \
  --password="" \
  migrate
