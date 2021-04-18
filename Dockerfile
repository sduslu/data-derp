FROM python:3.7

# Install OpenJDK 8
RUN apt update
RUN apt-get install -y software-properties-common
RUN wget -qO - https://adoptopenjdk.jfrog.io/adoptopenjdk/api/gpg/key/public | apt-key add -
RUN add-apt-repository -y https://adoptopenjdk.jfrog.io/adoptopenjdk/deb/
RUN apt-get update
RUN apt-get install -y adoptopenjdk-8-hotspot

# Install NodeJS (for Plotly - JupyterLab integration)
RUN curl -fsSL https://deb.nodesource.com/setup_15.x | bash -
RUN apt-get install -y nodejs

# Crowbar
# Not sure if this is going to work. Secret Service requires a GUI for some weird reason with:
# Secret Service Error: Dbus error: "Unable to autolaunch a dbus-daemon without a $DISPLAY for X11"
# AWS_PROFILE=twdu-germany aws s3 ls --region eu-central-1 fails
RUN apt-get install -y libdbus-1-dev libdbus-1-3 xvfb
RUN curl -L -o /usr/local/bin/crowbar \
    https://github.com/moritzheiber/crowbar/releases/download/v0.3.7/crowbar-x86_64-linux && \
    chmod +x /usr/local/bin/crowbar
    
# Install TWDU Germany libraries
COPY requirements.txt .
RUN pip install -r requirements.txt

# Install JupyterLab renderer support
RUN jupyter labextension install jupyterlab-plotly@4.14.3

# TWDU Germany environment variables
ENV TWDU_ENVIRONMENT=local