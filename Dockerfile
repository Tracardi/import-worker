FROM python:3.8

ENV VIRTUAL_ENV=/opt/venv
RUN python3 -m venv $VIRTUAL_ENV
ENV PATH="$VIRTUAL_ENV/bin:/app:$PATH"

RUN /opt/venv/bin/python3 -m pip install --upgrade pip

RUN mkdir app/
WORKDIR /app

COPY requirements.txt .
COPY worker ./worker

RUN pip --default-timeout=240 install -r requirements.txt

ENTRYPOINT ["celery", "-A","worker.celery_worker","worker", "-l", "info", "--uid=nobody", "--gid=nogroup"]
