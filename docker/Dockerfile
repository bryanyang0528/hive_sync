FROM mcapitanio/hive:1.2.1

ENV HIVE_SERVER2_THRIFT_PORT 10000

EXPOSE 9083 10000 10002 50111

VOLUME ["/opt/hadoop/etc/hadoop" "/opt/hive/conf", "/opt/hive/lib"]

ADD supervisord.conf /etc/supervisord.conf

ENTRYPOINT ["supervisord", "-c", "/etc/supervisord.conf", "-n"]
