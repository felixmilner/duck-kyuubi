# DuckDB Quack server: a pinned DuckDB JDBC build (keep it the same as the Kyuubi image's), the
# QuackServer.java launcher, and the extensions server.sql loads, so nothing is downloaded at runtime.
#   docker build -t duck-kyuubi/quack-server:0.1.0 quack-server
# Run it with the SQL to execute as its argument (e.g. server.sql, mounted from a ConfigMap) and the
# client token in $TOKEN.
ARG DUCKDB_JDBC_VERSION=2.0.0-alpha43385-881

FROM eclipse-temurin:21-jdk AS build
COPY QuackServer.java /src/
RUN javac -d /classes /src/QuackServer.java

FROM eclipse-temurin:21-jre
ARG DUCKDB_JDBC_VERSION
# must match the DuckDB build inside the jar (SELECT version())
ARG DUCKDB_VERSION=v2.0.0-alpha43385
ARG EXTENSIONS="quack iceberg httpfs avro"
ARG TARGETARCH
# DuckDB 2.0 builds (which Quack needs) are only on DuckDB's staging Maven repository so far
ADD https://duckdb-staging.duckdb.org/duckdb/duckdb-java/maven/org/duckdb/duckdb_jdbc/${DUCKDB_JDBC_VERSION}/duckdb_jdbc-${DUCKDB_JDBC_VERSION}.jar /opt/duckdb/duckdb_jdbc.jar
COPY --from=build /classes/ /opt/duckdb/
COPY server.sql /opt/duckdb/server.sql
# INSTALL looks in $HOME/.duckdb/extensions/<version>/<platform>/
ENV HOME=/opt/duckdb/home
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/* \
    && platform=linux_${TARGETARCH:-amd64} && dir=$HOME/.duckdb/extensions/$DUCKDB_VERSION/$platform \
    && mkdir -p $dir \
    && for e in $EXTENSIONS; do \
         curl -fsSL http://extensions.duckdb.org/$DUCKDB_VERSION/$platform/$e.duckdb_extension.gz \
           | gunzip > $dir/$e.duckdb_extension || exit 1; \
       done \
    && chmod 644 /opt/duckdb/duckdb_jdbc.jar && chown -R 10001:10001 $HOME

USER 10001
WORKDIR /opt/duckdb
EXPOSE 9494
ENTRYPOINT ["java", "-cp", "/opt/duckdb:/opt/duckdb/duckdb_jdbc.jar", "QuackServer"]
CMD ["/opt/duckdb/server.sql"]
