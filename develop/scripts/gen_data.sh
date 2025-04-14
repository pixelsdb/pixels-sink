tpch_data=${DEVELOP_DIR}/example/tpch_data
dbgen_path=${DEVELOP_DIR}/example/tpch-dbgen

tpcc_config=${DEVELOP_DIR}/example/tpcc_config
benchmark_tool=${DEVELOP_DIR}/example/benchmarksql

function  build_generator() {
  [[ -d ${dbgen_path} ]] || { log_fatal_exit "Failed to clone tpch_data into ${dbgen_path}. Please update submodule manually"; }

  WORK_PATH=`pwd`
  cd ${dbgen_path}
  make -j${nproc}
  check_fatal_exit "Failed to build tpch-dbgen_path"
  cd ${WORK_PATH}
}


function generate_tpch_data() {
  SCALE=${1:-"0.1"}
  log_info "DBGEN: Scale ${SCALE}"
  [[ -d ${tpch_data} ]] || { log_fatal_exit "Failed to find dir ${tpch_data}"; }

  WORK_PATH=`pwd`
  cd ${dbgen_path}
  DBGEN=${dbgen_path}/dbgen
  [[ -x ${DBGEN} ]] || { log_fatal_exit "Failed to execute ${DBGEN}. Check build process"; }
  eval `${DBGEN} -vf -s ${SCALE}`
  check_fatal_exit "Failed to generate data"
  mv -f *.tbl ${tpch_data} # TODO:Solve the tbl permission issues

  cd ${tpch_data}
  ls *.tbl | xargs md5sum >tpch_data.md5sum
  check_warning "Failed to calculate the md5sum of TPCH DATA" # If run it repeatedly, this may fail
  cd ${WORK_PATH}
}

function clean_tpch_data() {
  rm -f ${tpch_data}/*.tbl ${tpch_data}/tpch_data.md5sum
}

function build_tpcc_tool() {
    ant -f ${benchmark_tool}/build.xml
    check_fatal_exit "Can't build tpc-c benchmark tool"
}

function get_db_config_path() {
    local db_type=$1
    
    case "$db_type" in
        mysql|MySQL|MYSQL)
            echo "${tpcc_config}/props.mysql"
            ;;
        pg|postgres|postgresql|PostgreSQL|PG)
            echo "${tpcc_config}/props.pg"
            ;;
        *)
            log_fatal "Error: Unsupported database type '$db_type'"
            log_fatal "Usage: $0 {mysql|pg}"
            exit 1
            ;;
    esac
}

function build_tpcc_db() {
    local db_type=$1
    local config_path
    
    config_path=$(get_db_config_path "$db_type")
    
    log_info "Building TPC-C database for $db_type using config: $config_path"
  
    local WORK_PATH=`pwd`
    cd ${benchmark_tool}/run
    ./runDatabaseBuild.sh $config_path
    check_fatal_exit "Building TPC-C database for $db_type failed"
    cd ${WORK_PATH}
}

function start_tpcc_test() {
    local db_type=$1
    local config_path
    
    config_path=$(get_db_config_path "$db_type")
    
    log_info "Run TPC-C benchmakr for $db_type using config: $config_path"
  
    local WORK_PATH=`pwd`
    cd ${benchmark_tool}/run
    ./runBenchmark.sh $config_path
    check_fatal_exit "Run TPC-C Benchmark for $db_type failed"
    cd ${WORK_PATH}
}