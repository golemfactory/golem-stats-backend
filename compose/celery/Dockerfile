# pull official python alpine image
FROM ubuntu:latest
ARG DEBIAN_FRONTEND=noninteractive


# Making source and static directory
RUN mkdir /stats-backend
RUN mkdir /stats-backend/static

# Creating Work Directory
WORKDIR /stats-backend

# Adding mandatory packages to docker
RUN apt update && apt install -y \
    postgresql \
    curl \
    git \
    python3 \
    python3-pip \
    openssh-server
# Installing temporary packages required for installing requirements.pip 
RUN apt install -y build-essential \
    python3-dev \ 
    libffi-dev \ 
    libpq-dev \
    jq \
    netcat \
    musl-dev 

# Update pip
RUN pip3 install --upgrade pip

# Installing requirements.pip from project
COPY ./requirements.pip /scripts/
RUN pip3 install --no-cache-dir -r /scripts/requirements.pip

#RUN cd /stats-backend/ && cd yajsapi && git checkout b0.3 && cd examples/ && yarn


RUN mkdir -p $HOME/.local/share/ya-installer/terms
RUN touch $HOME/.local/share/ya-installer/terms/testnet-01.tag
RUN curl -sSf https://join.golem.network/as-provider | YA_INSTALLER_CORE=pre-rel-v0.6.6-rc3 bash -
ENV PATH=${PATH}:/root/.local/bin/:/root/.local/
# CMD will run when this dockerfile is running
CMD ["bash", "-c", "/wait-for-it.sh postgres:5432 -- /stats-backend/start.sh; celery -A core worker -l INFO"]
