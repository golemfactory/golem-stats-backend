 
DJANGO   := phillipjensen/golem-stats-backend
IMG_DJANGO    := ${DJANGO}:${GITHUB_SHA}
LATEST_DJANGO := ${DJANGO}:latest

CELERY   := phillipjensen/golem-stats-backend-celery
IMG_CELERY    := ${CELERY}:${GITHUB_SHA}
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
	@docker login -u ${DOCKER_USER} -p ${DOCKER_PASS}
	