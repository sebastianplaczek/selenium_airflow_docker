# Use the official Airflow image
FROM apache/airflow:2.5.0-python3.8

# Install Firefox, Geckodriver, and wget
USER root
RUN apt-get update \
    && apt-get install -y firefox-esr wget \
    && wget -O /tmp/geckodriver-v0.30.0-linux64.tar.gz https://github.com/mozilla/geckodriver/releases/download/v0.30.0/geckodriver-v0.30.0-linux64.tar.gz \
    && tar -xzf /tmp/geckodriver-v0.30.0-linux64.tar.gz -C /usr/local/bin/ \
    && chmod +x /usr/local/bin/geckodriver \
    && rm /tmp/geckodriver-v0.30.0-linux64.tar.gz




# Set the user back to airflow
USER airflow

# Install Selenium
RUN pip install --upgrade pip
RUN pip install selenium
RUN pip install PyMySQL==1.1.1
RUN pip install PyYAML
RUN pip install openpyxl
