# Multi-stage build for dlq-manager-server. Build context is the repo root.
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /workspace
COPY pom.xml .
COPY dlq-manager-starter/pom.xml dlq-manager-starter/
COPY dlq-manager-server/pom.xml dlq-manager-server/
COPY examples/demo-failing-consumer/pom.xml examples/demo-failing-consumer/
COPY dlq-manager-starter/src dlq-manager-starter/src
COPY dlq-manager-server/src dlq-manager-server/src
RUN mvn -q -pl dlq-manager-server -am -DskipTests package

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /workspace/dlq-manager-server/target/dlq-manager-server-*.jar app.jar
EXPOSE 8090
ENTRYPOINT ["java", "-jar", "app.jar"]
