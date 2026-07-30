# Build stage
FROM maven:3.9.6-eclipse-temurin-21 AS build
WORKDIR /app

# Copy the entire parent and modules context
COPY . .

# Compile and package the service along with its local common dependency
RUN mvn clean package -DskipTests -pl sorting-warehouse-service -am

# Run stage
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY --from=build /app/sorting-warehouse-service/target/sorting-warehouse-service-*.jar app.jar
EXPOSE 8081
ENTRYPOINT ["java", "-jar", "app.jar"]
