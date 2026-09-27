FROM maven:3.9.11-eclipse-temurin-17 AS test
WORKDIR /build
COPY pom.xml .
RUN mvn -B dependency:go-offline
COPY src ./src
RUN mvn -B verify

FROM flink:1.20.2-scala_2.12-java17
COPY --from=test /build/target/fraud-detection-job.jar /opt/flink/usrlib/fraud-detection-job.jar
