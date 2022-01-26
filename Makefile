DJANGO   := phillipjensen/golem-stats-backend
IMG_DJANGO    := golem-stats-backend:${GITHUB_SHA}
DJANGO_LATEST_LOCAL    := golem-stats-backend:latest
LATEST_DJANGO := ${DJANGO}:${GITHUB_SHA}

CELERY   := phillipjensen/golem-stats-backend-celery
IMG_CELERY    := golem-stats-backend-celery:${GITHUB_SHA}
CELERY_LATEST_LOCAL    := golem-stats-backend-celery:latest
LATEST_CELERY := ${CELERY}:${GITHUB_SHA}

CELERY_BEAT   := phillipjensen/golem-stats-backend-celery-beat
IMG_CELERY_BEAT    := golem-stats-backend-celery-beat:${GITHUB_SHA}
CELERY_BEAT_LATEST_LOCAL    := golem-stats-backend-celery-beat:latest
LATEST_CELERY_BEAT := ${CELERY_BEAT}:${GITHUB_SHA}

YAGNA   := phillipjensen/golem-stats-backend-celery-yagna
IMG_YAGNA    := golem-stats-backend-celery-yagna:${GITHUB_SHA}
YAGNA_LATEST_LOCAL    := golem-stats-backend-celery-yagna:latest
LATEST_YAGNA := ${YAGNA}:${GITHUB_SHA}

build-amd64:
	@docker build -t ${IMG_DJANGO} -t ${DJANGO_LATEST_LOCAL} -f ./dockerfiles/Django .
	@docker build -t ${IMG_CELERY} -t ${CELERY_LATEST_LOCAL} -f ./dockerfiles/Celery .
	@docker build -t ${IMG_CELERY_BEAT} -t ${CELERY_BEAT_LATEST_LOCAL} -f ./dockerfiles/Beat .
	@docker build -t ${IMG_YAGNA} -t ${YAGNA_LATEST_LOCAL} -f ./dockerfiles/Yagna .
	@docker tag ${IMG_DJANGO} ${LATEST_DJANGO}
	@docker tag ${IMG_CELERY} ${LATEST_CELERY}
	@docker tag ${IMG_CELERY_BEAT} ${LATEST_CELERY_BEAT}
	@docker tag ${IMG_YAGNA} ${LATEST_YAGNA}

	@docker tag ${DJANGO_LATEST_LOCAL} ${DJANGO}:latest
	@docker tag ${CELERY_LATEST_LOCAL} ${CELERY}:latest
	@docker tag ${CELERY_BEAT_LATEST_LOCAL} ${CELERY_BEAT}:latest
	@docker tag ${YAGNA_LATEST_LOCAL} ${YAGNA}:latest

push-amd64:
	@docker push ${LATEST_DJANGO}
	@docker push ${LATEST_CELERY}
	@docker push ${LATEST_CELERY_BEAT}
	@docker push ${LATEST_YAGNA}
	@docker push ${DJANGO}:latest
	@docker push ${CELERY}:latest
	@docker push ${CELERY_BEAT}:latest
	@docker push ${YAGNA}:latest


login:
	@docker login -u ${DOCKER_USER} -p ${DOCKER_PASS}
	
