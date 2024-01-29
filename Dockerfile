# FROM apache/airflow:latest

# USER root

# # Install required packages
# RUN apt-get update && \
#     apt-get -y install git unixodbc-dev && \
#     apt-get clean

# # Download and install the Microsoft ODBC Driver for SQL Server
# RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
#     curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
#     apt-get update && \
#     ACCEPT_EULA=Y apt-get -y install msodbcsql17 && \
#     apt-get clean

# # Set permissions on specific directories
# RUN chmod -R 777 /opt/airflow

# USER airflow

FROM apache/airflow:latest

USER root

# Install required packages
RUN apt-get update && \
    apt-get -y install git unixodbc-dev && \
    apt-get clean

# Download and install the Microsoft ODBC Driver for SQL Server
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get -y install msodbcsql17 && \
    apt-get clean

# Set permissions on specific directories
RUN chmod -R 777 /opt/airflow

USER airflow

# Install SQLAlchemy with a specific version
RUN pip install SQLAlchemy==1.4.25