FROM registry.access.redhat.com/ubi8/ubi
USER root


RUN yum update

RUN yum install python36

RUN yum clean all


RUN pip3 install redis


ENTRYPOINT ["/bin/bash"]
