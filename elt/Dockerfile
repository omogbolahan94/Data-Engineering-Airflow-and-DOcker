FROM python:3.8-slim

RUN apt-get update && apt-get install -y postgresql-client

# copy all the content of the python script so that docker will have access to it
COPY elt_script.py .

CMD ["python", "elt_script.py"]