# Use the official Airflow image
FROM apache/airflow:2.5.0-python3.8

ARG CACHE_BUST=$(date +%s)
ENV CACHE_BUST=${CACHE_BUST}

# Install Firefox, Geckodriver, and wget
USER root
RUN apt-get update \
    && apt-get install -y firefox-esr wget \
    && wget -O /tmp/geckodriver-v0.30.0-linux64.tar.gz https://github.com/mozilla/geckodriver/releases/download/v0.30.0/geckodriver-v0.30.0-linux64.tar.gz \
    && tar -xzf /tmp/geckodriver-v0.30.0-linux64.tar.gz -C /usr/local/bin/ \
    && chmod +x /usr/local/bin/geckodriver \
    && rm /tmp/geckodriver-v0.30.0-linux64.tar.gz \
    && apt-get install -y git




# Set the user back to airflow
USER airflow

ARG CACHE_BUST=$(date +%s)

# Install Python packages (cache-busted)
RUN pip install --upgrade pip \
    && pip install selenium \
    && pip install PyMySQL==1.1.1 \
    && pip install PyYAML \
    && pip install openpyxl \
    && pip install git+https://gitlab.com/my_projects2580645/scrapers.git@main

