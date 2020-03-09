FROM python:3.6

RUN apt-get update
RUN apt-get -y upgrade
RUN apt-get -y install nano vim git python3-pydot python-pydot python-pydot-ng graphviz python3-tk zip unzip curl ftp fail2ban python3-openssl

RUN pip install --no-cache-dir -U pm4pymdl==0.0.15
RUN apt-get -y install xdg-utils

COPY . /app

WORKDIR /app

ENTRYPOINT [ "python3" ]

CMD [ "main.py" ]
