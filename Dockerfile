FROM python:3.7

# Install OpenJDK 8
RUN apt update
RUN apt-get install --yes software-properties-common
RUN wget -qO - https://adoptopenjdk.jfrog.io/adoptopenjdk/api/gpg/key/public | apt-key add -
RUN add-apt-repository --yes https://adoptopenjdk.jfrog.io/adoptopenjdk/deb/
RUN apt-get update
RUN apt-get install --yes adoptopenjdk-8-hotspot

# Install NodeJS (for Plotly - JupyterLab integration)
RUN curl -fsSL https://deb.nodesource.com/setup_15.x | bash -
RUN apt-get install --yes nodejs

# Install TWDU Germany libraries
COPY requirements.txt .
RUN pip install -r requirements.txt

# Install JupyterLab renderer support
RUN jupyter labextension install jupyterlab-plotly@4.14.3

# TWDU Germany environment variables
ENV TWDU_ENVIRONMENT=local
