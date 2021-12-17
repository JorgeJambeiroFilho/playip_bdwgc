FROM jesjf/jjprivate:mssql_openssl111

# Set the working directory to /app
WORKDIR /app

# Copy the current directory contents into the container at /app
ADD . /app

# Install any needed packages
#RUN python setup.py develop

RUN pip install -r requirements.txt


