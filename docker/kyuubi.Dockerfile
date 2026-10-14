# Apache Kyuubi with the duck-kyuubi plugins, for amd64 or arm64. Build from the repository root:
#   docker build -f docker/kyuubi.Dockerfile -t duck-kyuubi/kyuubi:0.1.0 .
# Layout: the dialects and DuckDB's JDBC driver in /opt/kyuubi/externals/duck-kyuubi (set
# kyuubi.engine.jdbc.extra.classpath to it), the session policy in /opt/kyuubi/jars (the server's
# classpath). Runs `kyuubi run` as uid 10009; mount your kyuubi-defaults.conf in /opt/kyuubi/conf.
ARG KYUUBI_VERSION=1.12.0

FROM maven:3.9-eclipse-temurin-17 AS plugins
ARG KYUUBI_VERSION
WORKDIR /src
COPY pom.xml .
COPY kyuubi-duckdb kyuubi-duckdb
COPY kyuubi-launcher kyuubi-launcher
COPY kyuubi-session-policy kyuubi-session-policy
# compiled against the Kyuubi it runs in: the dialect API is internal
RUN mvn -q -B -DskipTests -Dkyuubi.version=${KYUUBI_VERSION} package

FROM eclipse-temurin:17-jre
ARG KYUUBI_VERSION
# Quack needs a DuckDB 2.0 build, only on DuckDB's staging Maven repository so far. Keep it the same as
# quack-server/Dockerfile. (For the launcher alone, the stable org.duckdb:duckdb_jdbc from Maven Central works.)
ARG DUCKDB_JDBC_VERSION=2.0.0-alpha43385-881
# must match the DuckDB build inside the jar (SELECT version())
ARG DUCKDB_VERSION=v2.0.0-alpha43385
ARG TARGETARCH
ENV KYUUBI_HOME=/opt/kyuubi
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/* \
    && (curl -fsSL https://downloads.apache.org/kyuubi/kyuubi-${KYUUBI_VERSION}/apache-kyuubi-${KYUUBI_VERSION}-bin.tgz \
        || curl -fsSL https://archive.apache.org/dist/kyuubi/kyuubi-${KYUUBI_VERSION}/apache-kyuubi-${KYUUBI_VERSION}-bin.tgz) \
       | tar -xz -C /opt \
    && mv /opt/apache-kyuubi-${KYUUBI_VERSION}-bin $KYUUBI_HOME \
    && mkdir -p $KYUUBI_HOME/externals/duck-kyuubi \
    && curl -fsSL -o $KYUUBI_HOME/externals/duck-kyuubi/duckdb_jdbc.jar \
       https://duckdb-staging.duckdb.org/duckdb/duckdb-java/maven/org/duckdb/duckdb_jdbc/${DUCKDB_JDBC_VERSION}/duckdb_jdbc-${DUCKDB_JDBC_VERSION}.jar
# The extensions the engine's DuckDB uses, so it never downloads them: quack, and httpfs for DuckDB's secure
# crypto module. The JDBC engine inherits HOME, and DuckDB looks in $HOME/.duckdb/extensions/<version>/<platform>/
ENV HOME=/opt/kyuubi/home
RUN platform=linux_${TARGETARCH:-amd64} && dir=$HOME/.duckdb/extensions/$DUCKDB_VERSION/$platform \
    && mkdir -p $dir \
    && for e in quack httpfs; do \
         curl -fsSL http://extensions.duckdb.org/$DUCKDB_VERSION/$platform/$e.duckdb_extension.gz \
           | gunzip > $dir/$e.duckdb_extension || exit 1; \
       done
COPY --from=plugins /src/kyuubi-duckdb/target/kyuubi-duckdb-*.jar /src/kyuubi-launcher/target/kyuubi-launcher-*.jar \
     /opt/kyuubi/externals/duck-kyuubi/
COPY --from=plugins /src/kyuubi-session-policy/target/kyuubi-session-policy-*.jar /opt/kyuubi/jars/
# writable: KYUUBI_HOME itself (the default metadata store, kyuubi_state_store.db), logs, work, pid, conf
RUN useradd --uid 10009 --home-dir $HOME --no-create-home kyuubi \
    && mkdir -p $KYUUBI_HOME/logs $KYUUBI_HOME/work $KYUUBI_HOME/pid \
    && chown 10009:10009 $KYUUBI_HOME $KYUUBI_HOME/logs $KYUUBI_HOME/work $KYUUBI_HOME/pid \
    && chown -R 10009:10009 $KYUUBI_HOME/home $KYUUBI_HOME/conf
USER 10009
WORKDIR /opt/kyuubi
EXPOSE 10009 10099
ENTRYPOINT ["/opt/kyuubi/bin/kyuubi", "run"]
