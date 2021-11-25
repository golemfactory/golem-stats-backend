 
DJANGO   := phillipjensen/golem-stats-backend
IMG_DJANGO    := golem-stats-backend:${GITHUB_SHA}
LATEST_DJANGO := ${DJANGO}:${GITHUB_SHA}

CELERY   := phillipjensen/golem-stats-backend-celery
IMG_CELERY    := golem-stats-backend-celery:${GITHUB_SHA}
LATEST_CELERY := ${CELERY}:${GITHUB_SHA}

CELERY_BEAT   := phillipjensen/golem-stats-backend-celery-beat
IMG_CELERY_BEAT    := golem-stats-backend-celery-beat:${GITHUB_SHA}
LATEST_CELERY_BEAT := ${CELERY_BEAT}:${GITHUB_SHA}
 
build:
	@docker build -t ${IMG_DJANGO} -f ./dockerfiles/Django .
	@docker build -t ${IMG_CELERY} -f ./dockerfiles/Celery .
	@docker build -t ${IMG_CELERY_BEAT} -f ./dockerfiles/Beat .
	@docker tag ${IMG_DJANGO} ${LATEST_DJANGO}
	@docker tag ${IMG_CELERY} ${LATEST_CELERY}
	@docker tag ${IMG_CELERY_BEAT} ${LATEST_CELERY_BEAT}

 
push:
	@docker push ${DJANGO}:${GITHUB_SHA}
	@docker push ${CELERY}:${GITHUB_SHA}
	@docker push ${CELERY_BEAT}:${GITHUB_SHA}
 
login:
	@docker login -u ${DOCKER_USER} -p ${DOCKER_PASS}
	