FROM tiangolo/uvicorn-gunicorn-fastapi:python3.8

# Set the working directory to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
ADD . /app

# Install any needed packages
#RUN python setup.py develop

RUN apt-get update \
  && apt-get -y install gcc gnupg2 \
  && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
  && curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list

RUN apt-get update \
  && ACCEPT_EULA=Y apt-get -y install msodbcsql17 \
  && ACCEPT_EULA=Y apt-get -y install mssql-tools

RUN wget https://www.openssl.org/source/openssl-1.1.1l.tar.gz -O openssl-1.1.1l.tar.gz && \
    tar -zxvf openssl-1.1.1l.tar.gz && cd openssl-1.1.1l && ./config && make&& \
    make install && ldconfig

RUN echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc \
  && echo 'export PATH="$PATH:/opt/mssql-tools/bin"' >> ~/.bashrc

RUN cat ~/.bashrc
RUN /bin/bash -c "source ~/.bashrc"

RUN apt-get -y install unixodbc-dev \
  && apt-get -y install python-pip \
  && pip install pyodbc

RUN pip install -r requirements.txt


