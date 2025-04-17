# Use an official Ubuntu as a parent image
FROM ubuntu:20.04

# Set environment variables
ENV DEBIAN_FRONTEND=noninteractive
ENV PATH="/app/venv/bin:$PATH"
ENV DISPLAY=:99

# Install dependencies
RUN apt-get update && \
    apt-get install -y software-properties-common && \
    add-apt-repository ppa:deadsnakes/ppa && \
    apt-get update && \
    apt-get install -y \
    python3.12 \
    python3.12-venv \
    python3.12-dev \
    openjdk-8-jdk \
    libcairo2-dev \
    libpango1.0-dev \
    libgdk-pixbuf2.0-dev \
    shared-mime-info \
    && apt-get clean

# Install SSH server
RUN apt-get -y install openssh-client openssh-server

# Set environment variable for SSH
RUN mkdir /var/run/sshd
RUN echo 'root:root' | chpasswd

# Install Xvfb for headless display
RUN apt-get install -y \
    xvfb \
    x11-utils \
    xfonts-base \
    xfonts-75dpi \
    xfonts-scalable \
    libxi6 \
    libgconf-2-4 \
    libnss3 \
    libxss1 \
    libappindicator1 \
    libindicator7 \
    && apt-get clean

# Set up virtual display
RUN mkdir -p /tmp/.X11-unix && chmod 1777 /tmp/.X11-unix

# Install Chrome & ChromeDriver
RUN apt-get update && apt-get install -y wget curl unzip && \
    wget -q -O - https://dl.google.com/linux/linux_signing_key.pub | apt-key add - && \
    echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" | tee /etc/apt/sources.list.d/google-chrome.list && \
    apt-get update && apt-get install -y google-chrome-stable

# Install ChromeDriver compatible with Chrome 133
RUN wget -q "https://storage.googleapis.com/chrome-for-testing-public/135.0.7049.84/linux64/chromedriver-linux64.zip" -O /tmp/chromedriver.zip && \
    unzip /tmp/chromedriver.zip -d /usr/local/bin/ && \
    mv /usr/local/bin/chromedriver-linux64/chromedriver /usr/local/bin/chromedriver && \
    chmod +x /usr/local/bin/chromedriver

# Set the working directory
WORKDIR /app

# Copy the current working directory contents into the container at /app
COPY app /app/app
COPY src /app/src
COPY eSignerJava /app/eSignerJava
COPY requirements.txt /app
COPY .env /app
COPY api-google.json /app/
COPY utilities.sh /app
COPY startup.sh /app

# Install Python dependencies inside venv
RUN python3.12 -m venv /app/venv && \
    /app/venv/bin/pip install --upgrade pip && \
    /app/venv/bin/pip install -r requirements.txt

# Verify installations
RUN python3.12 --version
RUN java -version
RUN google-chrome --version
RUN chromedriver --version

RUN chmod +x utilities.sh
RUN ./utilities.sh
# Set default command
RUN service ssh restart
# RUN Xvfb :99 -screen 0 1920x1080x24 &
# RUN java -jar eSignerJava/eSignerJava.jar &
# CMD ["tail", "-f", "/dev/null"]
CMD ["./startup.sh"]
