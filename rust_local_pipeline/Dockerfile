# Debian-based JRE/JDK 21 (glibc), not Alpine
FROM eclipse-temurin:21-jre-jammy

ENV MB_PLUGINS_DIR=/home/plugins

RUN mkdir -p /home/plugins && chmod 755 /home/plugins

# Metabase + DuckDB driver (pin versions)
ADD https://downloads.metabase.com/v0.56.8.x/metabase.jar /home/metabase.jar
ADD https://github.com/motherduckdb/metabase_duckdb_driver/releases/download/0.4.1/duckdb.metabase-driver.jar /home/plugins/duckdb.metabase-driver.jar


RUN curl https://install.duckdb.org | sh
# Pre-install spatial (downloads the binary into the image)
RUN /root/.duckdb/cli/latest/duckdb -c "INSTALL spatial;"

RUN chmod 744 /home/plugins/duckdb.metabase-driver.jar && java -version

EXPOSE 3000
CMD ["java", "-jar", "/home/metabase.jar"]
