# This docker file should be built in project source dir
FROM openjdk:8u92-jdk-alpine

WORKDIR /app

ARG jarFile
ARG IMAGE_PATH=develop/images/pixels-sink

ENV JAR_FILE=${jarFile}

COPY target/${jarFile} /app/
ADD ${IMAGE_PATH}/pixels-sink.properties ${IMAGE_PATH}/start.sh /app/
RUN chmod +x /app/start.sh
CMD ["./start.sh"]

LABEL authors="anti"
