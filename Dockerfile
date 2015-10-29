# http://github.com/broadinstitute/scala-baseimage
FROM broadinstitute/scala-baseimage

# Cromwell's HTTP Port
EXPOSE 8000

# Install Cromwell and add it as a service so it starts when the container starts
ADD . /cromwell
RUN /cromwell/docker/install.sh /cromwell && \
    mkdir /etc/service/cromwell && \
    cp /cromwell/docker/run.sh /etc/service/cromwell/run

# Install MySQL and add it as a service.  This is not required
RUN /cromwell/docker/mysql_install.sh /cromwell && \
    mkdir /etc/service/mysql && \
    echo "service mysql start" > /etc/service/mysql/run

# These next 4 commands are for enabling SSH to the container.
# id_rsa.pub is referenced below, but this should be any public key
# that you want to be added to authorized_keys for the root user.
# Copy the public key into this directory because ADD cannot reference
# Files outside of this directory

#EXPOSE 22
#RUN rm -f /etc/service/sshd/down
#ADD id_rsa.pub /tmp/id_rsa.pub
#RUN cat /tmp/id_rsa.pub >> /root/.ssh/authorized_keys
