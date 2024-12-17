FROM rust:1.79

RUN apt-get update && apt-get install -y python3-dev python3-venv

RUN python3 -m venv /opt/venv

ENV PATH="/opt/venv/bin:$PATH"

ENV VIRTUAL_ENV=/opt/venv

RUN pip install --no-cache-dir --upgrade pip

WORKDIR /opt

CMD tail -f /dev/null
