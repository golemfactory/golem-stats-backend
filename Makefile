DJANGO   := ghcr.io/golemfactory/golem-stats-backend
IMG_DJANGO    := golem-stats-backend:${GITHUB_SHA}
DJANGO_LATEST_LOCAL    := golem-stats-backend:latest
LATEST_DJANGO := ${DJANGO}:${GITHUB_SHA}

CELERY   := ghcr.io/golemfactory/golem-stats-backend-celery
IMG_CELERY    := golem-stats-backend-celery:${GITHUB_SHA}
CELERY_LATEST_LOCAL    := golem-stats-backend-celery:latest
LATEST_CELERY := ${CELERY}:${GITHUB_SHA}

CELERY_BEAT   := ghcr.io/golemfactory/golem-stats-backend-celery-beat
IMG_CELERY_BEAT    := golem-stats-backend-celery-beat:${GITHUB_SHA}
CELERY_BEAT_LATEST_LOCAL    := golem-stats-backend-celery-beat:latest
LATEST_CELERY_BEAT := ${CELERY_BEAT}:${GITHUB_SHA}

YAGNA   := ghcr.io/golemfactory/golem-stats-backend-celery-yagna
IMG_YAGNA    := golem-stats-backend-celery-yagna:${GITHUB_SHA}
YAGNA_LATEST_LOCAL    := golem-stats-backend-celery-yagna:latest
LATEST_YAGNA := ${YAGNA}:${GITHUB_SHA}


YAGNA_HYBRID   := ghcr.io/golemfactory/golem-stats-backend-celery-yagna-hybrid
IMG_YAGNA_HYBRID    := golem-stats-backend-celery-yagna-hybrid:${GITHUB_SHA}
YAGNA_HYBRID_LATEST_LOCAL    := golem-stats-backend-celery-yagna-hybrid:latest
LATEST_YAGNA_HYBRID := ${YAGNA_HYBRID}:${GITHUB_SHA}

YAGNA_HYBRID_TESTNET   := ghcr.io/golemfactory/golem-stats-backend-celery-yagna-hybrid-testnet
IMG_YAGNA_HYBRID_TESTNET    := golem-stats-backend-celery-yagna-hybrid-testnet:${GITHUB_SHA}
YAGNA_HYBRID_TESTNET_LATEST_LOCAL    := golem-stats-backend-celery-yagna-hybrid-testnet:latest
LATEST_YAGNA_HYBRID_TESTNET := ${YAGNA_HYBRID_TESTNET}:${GITHUB_SHA}

build-amd64:
	@docker build -t ${IMG_DJANGO} -t ${DJANGO_LATEST_LOCAL} -f ./dockerfiles/Django .
	@docker build -t ${IMG_CELERY} -t ${CELERY_LATEST_LOCAL} -f ./dockerfiles/Celery .
	@docker build -t ${IMG_CELERY_BEAT} -t ${CELERY_BEAT_LATEST_LOCAL} -f ./dockerfiles/Beat .
	@docker build -t ${IMG_YAGNA} -t ${YAGNA_LATEST_LOCAL} -f ./dockerfiles/Yagna .
	@docker build -t ${IMG_YAGNA_HYBRID} -t ${YAGNA_HYBRID_LATEST_LOCAL} -f ./dockerfiles/YagnaHybrid .
	@docker build -t ${IMG_YAGNA_HYBRID_TESTNET} -t ${YAGNA_HYBRID_TESTNET_LATEST_LOCAL} -f ./dockerfiles/YagnaHybridTestnet .
	@docker tag ${IMG_DJANGO} ${LATEST_DJANGO}
	@docker tag ${IMG_CELERY} ${LATEST_CELERY}
	@docker tag ${IMG_CELERY_BEAT} ${LATEST_CELERY_BEAT}
	@docker tag ${IMG_YAGNA} ${LATEST_YAGNA}
	@docker tag ${IMG_YAGNA_HYBRID} ${LATEST_YAGNA_HYBRID}
	@docker tag ${IMG_YAGNA_HYBRID_TESTNET} ${LATEST_YAGNA_HYBRID_TESTNET}

	@docker tag ${DJANGO_LATEST_LOCAL} ${DJANGO}:latest
	@docker tag ${CELERY_LATEST_LOCAL} ${CELERY}:latest
	@docker tag ${CELERY_BEAT_LATEST_LOCAL} ${CELERY_BEAT}:latest
	@docker tag ${YAGNA_LATEST_LOCAL} ${YAGNA}:latest
	@docker tag ${YAGNA_HYBRID_LATEST_LOCAL} ${YAGNA_HYBRID}:latest
	@docker tag ${YAGNA_HYBRID_TESTNET_LATEST_LOCAL} ${YAGNA_HYBRID_TESTNET}:latest

push-amd64:
	@docker push ${LATEST_DJANGO}
	@docker push ${LATEST_CELERY}
	@docker push ${LATEST_CELERY_BEAT}
	@docker push ${LATEST_YAGNA}
	@docker push ${LATEST_YAGNA_HYBRID}
	@docker push ${LATEST_YAGNA_HYBRID_TESTNET}
	@docker push ${DJANGO}:latest
	@docker push ${CELERY}:latest
	@docker push ${CELERY_BEAT}:latest
	@docker push ${YAGNA}:latest
	@docker push ${YAGNA_HYBRID}:latest
	@docker push ${YAGNA_HYBRID_TESTNET}:latest


login:
	@docker login ghcr.io -u ${DOCKER_USER} -p ${DOCKER_PASS}
	
