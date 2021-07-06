# COVID19-Data-Ticker
A module that constantly gets the latest COVID-19 data from a REST API and uploads it to the Google Storage Bucket using PubSub.

COVID-19 API : [COVID-19 Tracking API Documentation](https://rapidapi.com/slotixsro-slotixsro-default/api/covid-19-tracking/)

---

To run this module,

1. Install [Docker](https://www.docker.com/)
2. Clone this repository and navigate to [folder](https://github.com/sachinmb27/COVID19-Data-Ticker/blob/main/app/Dockerfile) that contains the DockerFile.
3. Build the docker image

    `docker build -t <DOCKER-IMAGE-NAME> .`
4. View the available docker images

    `docker images`
5. Run the docker image

    `docker run -it <DOCKER_IMAGE_NAME>`

---
