FROM openjdk:8-alpine
RUN mkdir /data
VOLUME /data
COPY target/syncer-core-1.0-SNAPSHOT.jar /syncer.jar
COPY bin/start.sh /start.sh
COPY bin/healthchecker.sh /healthchecker.sh
EXPOSE 40000 40000
HEALTHCHECK --interval=1m --timeout=3s --retries=4 CMD /healthchecker.sh
ENTRYPOINT ["sh", "start.sh"]