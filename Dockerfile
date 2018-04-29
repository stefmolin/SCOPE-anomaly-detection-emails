FROM debian:stretch
# Install R-related packages
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get -qq update -y > /dev/null && apt-get -q -y install r-base r-cran-rjava > /dev/null
RUN apt-get -qq update -y > /dev/null && apt-get -q -y install libcurl4-openssl-dev libssl-dev > /dev/null
RUN echo "r <- getOption('repos'); r['CRAN'] <- 'http://cran.us.r-project.org'; options(repos = r);" > ~/.Rprofile
RUN Rscript -e "install.packages('devtools')" > /dev/null
RUN Rscript -e "install.packages('R.utils')" > /dev/null
RUN Rscript -e "install.packages('digest')" > /dev/null
RUN Rscript -e "install.packages('reshape2')" > /dev/null
RUN Rscript -e "install.packages('ggplot2')" > /dev/null
RUN Rscript -e "install.packages('dplyr')" > /dev/null
RUN Rscript -e "install.packages('mailR')" > /dev/null
RUN Rscript -e "install.packages('slackr')" > /dev/null
RUN Rscript -e "install.packages('RJDBC')" > /dev/null
RUN Rscript -e "install.packages('base64')" > /dev/null
RUN Rscript -e "install.packages('ggrepel')" > /dev/null
RUN Rscript -e "install.packages('yaml')" > /dev/null
RUN Rscript -e "install.packages('futile.logger')" > /dev/null
RUN Rscript -e "install.packages('outliers')" > /dev/null
RUN Rscript -e "install.packages('data.table')" > /dev/null
RUN Rscript -e "install.packages('stringr')" > /dev/null
RUN Rscript -e "install.packages('plyr')" > /dev/null
# Install Python-related packages
RUN rm /var/lib/apt/lists/httpredir.debian.org_debian_dists_*
RUN apt-get update -y
RUN apt-get -y install python3.5 python3-pip
RUN pip3 install schedule pyyaml jinja2==2.10 
# Create folders
RUN mkdir -p /storage
RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app
COPY . /usr/src/app
# Launch app
CMD [ "python3", "-u", "./__init__.py" ]
