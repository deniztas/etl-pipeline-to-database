FROM python:3.8.0-alpine

WORKDIR /usr/src/workspace

COPY . /usr/src/workspace/

# set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
ENV PYTHONPATH=/usr/src/workspace

RUN apk update && apk add postgresql-dev gcc python3-dev musl-dev build-base

# upgrade pip
RUN pip install --upgrade pip

# install dependencies
RUN pip install -r requirements.txt

RUN export LDFLAGS="-L/usr/local/opt/openssl/lib"

EXPOSE 5000

ENTRYPOINT ["python", "app/app.py"]