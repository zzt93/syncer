FROM openjdk:8-alpine
RUN mkdir /data
VOLUME /data
COPY DataGenerator.java /
COPY mysql_test.sql /
RUN javac /DataGenerator.java
ENTRYPOINT ["java", "DataGenerator"]