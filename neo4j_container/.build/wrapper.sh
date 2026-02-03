#!/bin/sh
neo4j-admin database dump --to-path=/data neo4j
/usr/local/bin/docker-entrypoint.sh neo4j &
chown -R neo4j:neo4j /data/databases &
chmod -R 755 /data/databases &
tail -f /dev/null 