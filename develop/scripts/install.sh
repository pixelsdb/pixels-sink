#!/bin/bash

usage() {
  cat <<EOF


EOF
  exit 0;
}


develop_debug=off
need_init=on
need_build=on
load_example_data=on
example_data_scale=0.1

for arg do
  val=`echo "$arg" | sed -e 's;^--[^=]*=;;'`

  case "$arg" in
    --develop_debug=*)          develop_debug="$val";;
    --debug)                    develop_debug=on ;;
    --need_init=*)              need_init="$val";;
    --need_build=*)             need_build="$val" ;;
    -h|--help)                  usage ;;
    *)                          echo "wrong options : $arg";
                                exit 1
                                ;;
  esac
done

[[ x${develop_debug} == x"on" ]] && { set -o verbose -o xtrace; }

BASH_PATH=`readlink -f $0`
SCRIPT_DIR=`dirname ${BASH_PATH}`



source ${SCRIPT_DIR}/common_func.sh

if [[ x${need_build} == x"on" ]]; then 
  build_image ${IMAGE_DIR}/debezium-source-connector pixels-debezium
  build_pixels_sink_image
fi

if [[ x${load_example_data} == x"on" ]]; then
  build_generator
  generate_tpch_data ${example_data_scale}
fi

if [[ x${need_init} == x"on" ]]; then
  log_info "Init Container"
  docker compose -f ${DEVELOP_DIR}/docker-compose.yml up -d
  check_fatal_exit "docker-compose up failed."
fi

log_info "Containers Started"


log_info "Start Register Debezium Connectors"

log_info "Start Register MySQL Debezium Connector"
gen_config_by_template mysql_password $(cat "${SECRETS_DIR}/mysql-pixels-password.txt") ${CONFIG_DIR}/register-mysql.json.template
[[ -f ${CONFIG_DIR}/register-mysql.json ]] || { log_fatal_exit "Can't generate mysql debezium connector config"; }
wait_for_url http://localhost:8083/connectors 20
check_fatal_exit "MySQL Source Kafka Connector Server Fail"
# register mysql connector
try_command curl -f -X POST -H "Content-Type: application/json" -d @${CONFIG_DIR}/register-mysql.json http://localhost:8083/connectors -w '\n' # We need to wait here for MySQL to load all the data
check_fatal_exit "Register MySQL Source Connector Fail"

log_info "Start Register PostgreSQL Debezium Connector"
gen_config_by_template postgres_password $(cat "${SECRETS_DIR}/postgres-pixels-password.txt") ${CONFIG_DIR}/register-postgres.json.template
[[ -f ${CONFIG_DIR}/register-postgres.json ]] || { log_fatal_exit "Can't generate postgres debezium connector config"; }
wait_for_url http://localhost:8084/connectors
check_fatal_exit "Postgres Source Kafka Connector Server Fail"
try_command curl -f -X POST -H "Content-Type: application/json" -d @${CONFIG_DIR}/register-postgres.json http://localhost:8084/connectors -w '\n'
check_fatal_exit "Register PostgreSQL Source Connector Fail"

log_info "Visit http://localhost:9000 to check kafka status"

