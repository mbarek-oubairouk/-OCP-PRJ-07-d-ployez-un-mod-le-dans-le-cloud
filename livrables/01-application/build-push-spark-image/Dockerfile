from barkus/spark-py:v3.3.1-j11

WORKDIR /opt/spark/work-dir
user 0
RUN mkdir -p /opt/spark/work-dir/app/data/output
COPY requirements.txt .
RUN pip3 install -r requirements.txt
COPY app app
RUN chown -R 185:185 /opt/spark/work-dir
RUN mkdir /.local 
RUN chmod -R 777 /.local /opt/spark/work-dir
user 185

COPY  jars_extra/*.jar /opt/spark/jars/
ENV PYSPARK_MAJOR_PYTHON_VERSION=3
