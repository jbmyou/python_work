FROM ubuntu:20.04
RUN apt update && apt upgrade -y
RUN apt install python3 -y
RUN apt-get install python3-pip
WORKDIR /usr/src/test
COPY ["test.py", "."]
COPY ["file_function.py", "."]
COPY ["requirements.txt", "."]
RUN pip install --no-cache-dir --upgrade pip && pip install -r requirements.txtt