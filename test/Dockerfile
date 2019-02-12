FROM openjdk:8-alpine
RUN mkdir /data
VOLUME /data
COPY test/DataGenerator.java /DataGenerator.java
RUN javac /DataGenerator.java
ENTRYPOINT ["java", "DataGenerator", "/data"]