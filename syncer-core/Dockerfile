FROM openjdk:8-alpine
RUN mkdir /data
VOLUME /data
ADD ./syncer-core/target/syncer-core-1.0-SNAPSHOT.jar syncer.jar
EXPOSE 40000 40000
COPY ./script/bin/start.sh /start.sh
COPY ./script/healthchecker.sh /healthchecker.sh
HEALTHCHECK --interval=1m --timeout=3s --retries=4 CMD /healthchecker.sh
ENTRYPOINT ["bash", "start.sh", ""]