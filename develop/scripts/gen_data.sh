tpch_data=${DEVELOP_DIR}/example/tpch_data
dbgen_path=${DEVELOP_DIR}/example/tpch-dbgen

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