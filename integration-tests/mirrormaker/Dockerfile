FROM public.ecr.aws/bitnami/kafka:2.8
USER root
RUN install_packages gettext

RUN mkdir -p /opt/plugins
RUN chown 1234 /opt/plugins

ADD ./mm2-configs/connect-standalone.properties /opt/mm2/connect-standalone.properties
ADD ./mm2-configs/mirror-checkpoint-connector.properties /opt/mm2/mirror-checkpoint-connector.properties
ADD ./mm2-configs/mirror-heartbeat-connector.properties /opt/mm2/mirror-heartbeat-connector.properties
ADD ./mm2-configs/mirror-source-connector.properties /opt/mm2/mirror-source-connector.properties

ADD ./run.sh /opt/mm2/run.sh
RUN chmod +x /opt/mm2/run.sh

RUN mkdir -p /var/run/mm2
RUN chown 1234 /var/run/mm2

ENV TOPICS .*
ENV SOURCE "localhost:9092"
ENV DESTINATION "localhost:9093"
ENV ACLS_ENABLED "false"

USER 1234
CMD /opt/mm2/run.sh
