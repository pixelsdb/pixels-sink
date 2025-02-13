shutdown_containers() {
    docker compose  -f ${DEVELOP_DIR}/docker-compose.yml down -v
}

clean_images() {
    local images=("pixels-debezium:${PIXELS_SINK_VERSION}")

    for image in "${images[@]}"; do
        log_info "Deleting image: $image"
        docker rmi -f "$image" 2>/dev/null || echo "Failed to delete image: $image"
    done
}


