 
DJANGO   := phillipjensen/golem-stats-backend
IMG_DJANGO    := golem-stats-backend:${GITHUB_SHA}
LATEST_DJANGO := ${DJANGO}:${GITHUB_SHA}

CELERY   := phillipjensen/golem-stats-backend-celery
IMG_CELERY    := golem-stats-backend-celery:${GITHUB_SHA}
LATEST_CELERY := ${CELERY}:${GITHUB_SHA}
 
build:
	@docker build -t ${IMG_DJANGO} -f ./dockerfiles/Django .
	@docker build -t ${IMG_CELERY} -f ./dockerfiles/Celery .
	@docker tag ${IMG_DJANGO} ${LATEST_DJANGO}
	@docker tag ${IMG_CELERY} ${LATEST_CELERY}

 
push:
	@docker push ${DJANGO}:${GITHUB_SHA}
	@docker push ${CELERY}:${GITHUB_SHA}
 
login:
	@docker login -u ${DOCKER_USER} -p ${DOCKER_PASS}
	