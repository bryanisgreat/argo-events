FROM centos:7
RUN yum -y install ca-certificates
COPY dist/resource-gateway /bin/
ENTRYPOINT [ "/bin/resource-gateway" ]