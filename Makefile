GO111MODULE := on
DOCKER_TAG := $(or ${GITHUB_TAG_NAME}, latest)

all: s3-backup

.PHONY: s3-backup
s3-backup:
	go build s3-backup.go
	strip s3-backup

.PHONY: dockerimages
dockerimages: s3-backup
	docker build -t mwennrich/s3-backup:${DOCKER_TAG} . 

.PHONY: dockerpush
dockerpush: s3-backup
	docker push mwennrich/s3-backup:${DOCKER_TAG}
