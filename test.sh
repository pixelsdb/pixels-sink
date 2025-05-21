#
# Copyright 2025 PixelsDB.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
#
microdnf install -y vim

./develop/install --need_build=off --generate_data=off --enable_mysql=off --load_postgres=off
./develop/install --need_build=off --generate_data=off --enable_mysql=off
./develop/install --need_build=on --generate_data=off --enable_mysql=off

./develop/install --need_build=on --generate_data=off --enable_mysql=on --enable_postgres=off --enable_tpch=off
./develop/install --need_build=off --generate_data=off --enable_mysql=on --enable_postgres=off --enable_tpch=off --enable_tpcc=on

./develop/install --need_build=on --generate_data=off --enable_mysql=on --enable_postgres=off --enable_tpch=off --enable_tpcc=on

./develop/install --need_build=on --generate_data=on --data_scale=10 --enable_mysql=on --enable_postgres=off --enable_tpch=on --enable_tpcc=off

./develop/install --need_build=on --generate_data=on --data_scale=1 --enable_mysql=on --enable_postgres=off --enable_tpch=on --enable_tpcc=off

./develop/install --need_build=on --generate_data=on --data_scale=1 --enable_mysql=on --enable_postgres=off --enable_tpch=off --enable_tpcc=off


./develop/install --need_build=off --generate_data=off --enable_mysql=off --load_postgres=off

./develop/install --need_build=off --generate_data=off --enable_mysql=on --enable_postgres=off



####
docker exec pixels_mysql_source_db sh -c "mysql -upixels -p$(cat "${SECRETS_DIR}/mysql-pixels-password.txt") -D pixels_realtime_crud < /var/lib/mysql-files/sql/dss.ddl"
docker exec pixels_mysql_source_db sh -c "mysql -upixels -p$(cat "${SECRETS_DIR}/mysql-pixels-password.txt") -D pixels_realtime_crud < /var/lib/mysql-files/sql/sample.sql"