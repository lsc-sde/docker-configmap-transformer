FROM python:alpine
RUN apk add gcc
RUN pip install --upgrade pip 
RUN pip install kubernetes
RUN MULTIDICT_NO_EXTENSIONS=1 pip install kopf
ADD . /src
CMD kopf run /src/service.py -A --standalone
