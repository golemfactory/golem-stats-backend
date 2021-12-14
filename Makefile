 
DJANGO   := phillipjensen/golem-stats-backend
IMG_DJANGO    := ${DJANGO}:${GITHUB_SHA}

CELERY   := phillipjensen/golem-stats-backend-celery
IMG_CELERY    := ${CELERY}:${GITHUB_SHA}

CELERY_BEAT   := phillipjensen/golem-stats-backend-celery-beat
IMG_CELERY_BEAT    := ${CELERY_BEAT}:${GITHUB_SHA}
 
build:
	@docker buildx create --use	
	@docker buildx build --platform=linux/arm64,linux/amd64 --push -t ${IMG_DJANGO} -t ${DJANGO}:latest -f ./dockerfiles/Django .
	@docker buildx build --platform=linux/arm64,linux/amd64 --push -t ${IMG_CELERY} -t ${CELERY}:latest -f ./dockerfiles/Celery .
	@docker buildx build --platform=linux/arm64,linux/amd64 --push -t ${IMG_CELERY_BEAT} -t ${CELERY_BEAT}:latest -f ./dockerfiles/Beat .
 
login:
	@docker login -u ${DOCKER_USER} -p ${DOCKER_PASS}
	