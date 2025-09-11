FROM public.ecr.aws/bitnami/kafka:2.8
USER root
RUN install_packages gettext

RUN mkdir -p /opt/plugins
RUN chown 1234 /opt/plugins

RUN mkdir -p ~/.aws
RUN chmod 1234 ~/.aws

# Install the AWS CLI
RUN \
  apt-get update -y && \
  apt-get install -y wget vim python3 unzip python-is-python3 python3-venv && \
  wget "s3.amazonaws.com/aws-cli/awscli-bundle.zip" -O "awscli-bundle.zip" && \
  unzip awscli-bundle.zip && \
  ./awscli-bundle/install -i /usr/local/aws -b /usr/local/bin/aws && \
  rm awscli-bundle.zip && \
  rm -rf awscli-bundle

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
ENV AWS_PROFILE "default"

USER 1234
CMD /opt/mm2/run.sh
