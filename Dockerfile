FROM python:3.9.20-alpine3.20

RUN mkdir -p /nhl-stats-fetcher

WORKDIR /nhl-stats-fetcher

COPY /src ./scripts
COPY requirements.txt .
COPY crontab ./crontab/

RUN pip install -r requirements.txt

RUN crontab crontab/crontab

CMD ["crond", "-f", "-l", "2"]