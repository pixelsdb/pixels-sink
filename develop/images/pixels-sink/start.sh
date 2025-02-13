#!/bin/sh
# image entrypoint
set -o verbose -o xtrace



java -jar ${JAR_FILE} -c pixels-sink.properties