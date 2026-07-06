# Multi-stage build for the demo consumer. Build context is the repo root.
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /workspace
COPY pom.xml .
COPY dlq-manager-starter/pom.xml dlq-manager-starter/
COPY dlq-manager-server/pom.xml dlq-manager-server/
COPY examples/demo-failing-consumer/pom.xml examples/demo-failing-consumer/
COPY dlq-manager-starter/src dlq-manager-starter/src
COPY examples/demo-failing-consumer/src examples/demo-failing-consumer/src
RUN mvn -q -pl examples/demo-failing-consumer -am -DskipTests package

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /workspace/examples/demo-failing-consumer/target/demo-failing-consumer-*.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]
