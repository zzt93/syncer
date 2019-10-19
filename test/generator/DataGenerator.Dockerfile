FROM openjdk:8-alpine
RUN mkdir /data
VOLUME /data
COPY DataGenerator.java /
RUN javac /DataGenerator.java
ENTRYPOINT ["java", "DataGenerator"]