check_fatal_exit() {
    [[ $? -ne 0 ]] && { log_fatal_exit "$@";}
    return 0
}

check_warning() {
    [[ $? -ne 0 ]] && { log_warning "$@"; }
    return 0
}

check_return() {
    [[ $? -ne 0 ]] && { log_warning "$@" && exit 1; }
    return 0
}



gen_dir_md5sum() {
    local md5dir_path=$1
    if [ -z ${md5dir_path} ]; then
        md5dir_path="."
    fi
    [[ -d ${md5dir_path} ]] || { check_return "${md5dir_path} is not dir"; }
    find ${md5dir_path} -maxdepth 1  -type f| xargs md5sum  > ${md5dir_path}/md5sum.txt
}

# @1: placeholder
# @2: replacement
# @3: src_file (template)
# @4: dst_file
gen_config_by_template() {
    if [ "$#" -ne 3 ] && [ "$#" -ne 4 ]; then
        log_fatal_exit "Usage: gen_config_by_template <placeholder_name> <replacement_value> <source_file> <dst_file>"
    fi
    local placeholder_name="$1"
    local replacement_value="$2"
    local source_file="$3"

    if [ ! -f "$source_file" ]; then
        log_fatal_exit "Template file '$source_file' does not exist."
    fi
    
    
    local dst_file="${4:-${source_file%.template}}"

    sed "s/<\\$ ${placeholder_name}>/${replacement_value}/g" "$source_file" > "$dst_file"
}

wait_for_url() {
  local url=$1
  local retries=${2:-10}
  local interval=3

  for ((i=1; i<=retries; i++)); do
    if curl --silent --head --fail "$url" > /dev/null; then
      log_info "URL $url is ready."
      return 0
    else
      log_warning "Attempt $i/$retries: URL $url is not ready. Retrying in $interval seconds..."
      sleep $interval
    fi
  done

  log_fatal "URL $url did not become ready after $retries attempts."
  return 1
}

try_command() {
    local MAX_RETRIES=10
    local INTERVAL=6
    local COMMAND="$@"
    local count=0
    local STATUS

    while [ $count -lt $MAX_RETRIES ]; do
        log_info "Attempting to execute command: $COMMAND (Attempt $((count+1))/$MAX_RETRIES)"
        
        "$@"  # Execute the command using "$@" to pass the arguments as individual parameters
        STATUS=$?

        if [ $STATUS -eq 0 ]; then
            log_info "Command executed successfully!"
            return 0
        fi

        log_warning "Command failed, retrying in $INTERVAL seconds..."
        count=$((count + 1))
        sleep $INTERVAL
    done
    log_fatal "Max retries ($MAX_RETRIES) reached. Command failed."
    return 1
}