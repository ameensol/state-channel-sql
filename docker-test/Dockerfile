FROM centos:7
MAINTAINER David Wolever <david@wolever.net>

RUN yum install -y https://yum.postgresql.org/9.6/redhat/rhel-7-x86_64/pgdg-centos96-9.6-3.noarch.rpm
RUN yum install -y postgresql96-devel postgresql96-server postgresql96-contrib && yum clean all
RUN /usr/sbin/update-alternatives --install /usr/bin/pg_config pgsql-pg_config /usr/pgsql-9.6/bin/pg_config 930

ENV PATH="$PATH:/usr/pgsql-9.6/bin/"
RUN mkdir "/var/pgsql-9.6" && chown postgres "/var/pgsql-9.6"
RUN su postgres -c "initdb /var/pgsql-9.6/"

RUN curl -sL https://rpm.nodesource.com/setup_7.x | bash -
RUN yum install -y nodejs
