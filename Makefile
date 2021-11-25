 
DJANGO   := phillipjensen/golem-stats-backend
TAG    := $(shell git rev-parse HEAD)
IMG_DJANGO    := ${DJANGO}:${TAG}
LATEST_DJANGO := ${DJANGO}:latest

CELERY   := phillipjensen/golem-stats-backend-celery
TAG    := $(shell git rev-parse HEAD)
IMG_CELERY    := ${CELERY}:${TAG}
LATEST_CELERY := ${CELERY}:latest
 
build:
	@docker build -t ${IMG_DJANGO} -f ./dockerfiles/Django .
	@docker tag ${IMG_DJANGO} ${LATEST_DJANGO}
	@docker build -t ${IMG_CELERY} -f ./dockerfiles/Celery .
	@docker tag ${IMG_CELERY} ${LATEST_CELERY}

 
push:
	@docker push ${DJANGO}
	@docker push ${CELERY}
 
login:
	@docker log -u ${DOCKER_USER} -p ${DOCKER_PASS}
	