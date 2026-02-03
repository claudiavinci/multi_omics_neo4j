# FROM neo4j:latest
FROM neo4j:5.25.1

COPY wrapper.sh /usr/local/bin/
COPY docker-entrypoint.sh /usr/local/bin/

RUN chown root:root /usr/local/bin/wrapper.sh && \
    chmod 777 /usr/local/bin/wrapper.sh

RUN apt-get update && apt-get install -y gosu

RUN chown root:root /usr/local/bin/docker-entrypoint.sh && \
    chmod 777 /usr/local/bin/docker-entrypoint.sh

ENTRYPOINT [ "/usr/local/bin/wrapper.sh"]