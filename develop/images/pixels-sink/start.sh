#!/bin/sh
# image entrypoint
set -o verbose -o xtrace


JVM_OPTION="-Xmx4096m -Xmn1024m "
java $JVM_OPTION -jar ${JAR_FILE} -c pixels-sink.properties