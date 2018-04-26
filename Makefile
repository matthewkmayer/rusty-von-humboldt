# System Setup
SHELL = bash

# General Vars
APP := $(shell basename $(PWD) | tr '[:upper:]' '[:lower:]')
DATE := $(shell date -u +%Y-%m-%d%Z%H:%M:%S)
VERSION := 0.0.1

BUILD_TAG ?= local

TRAVIS_BUILD_NUMBER ?= 1
TRAVIS_COMMIT ?= $(shell git rev-parse HEAD)
TRAVIS_JOB_ID ?= $(BUILD_TAG)

BUILD_NUMBER ?= $(TRAVIS_BUILD_NUMBER)
BUILD_VERSION := $(VERSION)-$(BUILD_NUMBER)
GIT_COMMIT_HASH ?= $(TRAVIS_COMMIT)
JOB_ID ?= $(TRAVIS_JOB_ID)

DOCKER_VER_NUM := $(shell docker version --format '{{.Client.Version}}')
DOCKER_VER_MAJOR := $(shell echo $(DOCKER_VER_NUM) | cut -f1 -d.)
DOCKER_VER_MINOR := $(shell echo $(DOCKER_VER_NUM) | cut -f2 -d.)

AWS_CLI_OK := $(shell awsversion=$$(aws --version 2>&1 | cut -d " " -f 1 | cut -d "/" -f 2); if [ "$$(echo $$awsversion | cut -d "." -f 1)" -ge "1" ] && [ "$$(echo $$awsversion | cut -d "." -f 2)" -ge "11" ] && [ "$$(echo $$awsversion | cut -d "." -f 3)" -ge "92" ] || { [ "$(DOCKER_VER_MAJOR)" -gt "17" ] || { [ "$(DOCKER_VER_MAJOR)" -eq "17" ] && [ "$(DOCKER_VER_MINOR)" -ge "6" ]; } }; then echo OK; fi)
AWS_EMAIL_OPT := $(shell if [[ "$(AWS_CLI_OK)" == "OK" ]]; then echo "--no-include-email"; fi)

DOCKER_REPO ?= 313220119457.dkr.ecr.us-east-1.amazonaws.com/ionchannel
DOCKER_IMAGE_NAME ?= $(APP)
DOCKER_IMAGE_LABEL ?= latest
DOCKER_SERVICE_NAME := $(APP)-app-$(JOB_ID)

DEPLOY_ENV ?= testing
DEPLOY_DOCKER_LABEL ?= latest

NO_COLOR := \033[0m
INFO_COLOR := \033[0;36m

.PHONY: all
all: test build

.PHONY: build
build: dockerize ## Build the project
	packer build packer.json

.PHONY: dockerize
dockerize: ecr_login  ## Create a docker image of the project
	docker build \
		--build-arg APP_NAME=$(APP) \
		--build-arg BUILD_DATE=$(DATE) \
		--build-arg VERSION=$(BUILD_VERSION) \
		--build-arg GIT_COMMIT_HASH=$(GIT_COMMIT_HASH) \
		-t $(DOCKER_REPO)/$(DOCKER_IMAGE_NAME):$(DOCKER_IMAGE_LABEL) .

.PHONY: ecr_login
ecr_login:  ## Login to the ECR using local credentials
	@eval $$(aws ecr get-login --region us-east-1 $(AWS_EMAIL_OPT))

.PHONY: tag_image
tag_image: dockerize  ## Builds the image and tags it
	docker tag $(DOCKER_REPO)/$(DOCKER_IMAGE_NAME):$(DOCKER_IMAGE_LABEL) ionchannel/$(DOCKER_IMAGE_NAME)

.PHONY: run
run: tag_image ## Run a dockerized version of the app

.PHONY: deploy
deploy: deploy_images ## Deploys the code to the environment

.PHONY: deploy_images
deploy_images:  ## Properly tag and push images for deployment
	curl -s https://s3.amazonaws.com/public.ionchannel.io/files/scripts/travis_create_ecr_repo.sh | bash -s ionchannel/$(DOCKER_IMAGE_NAME) $(DEPLOY_DOCKER_LABEL)
	docker tag $(DOCKER_REPO)/$(DOCKER_IMAGE_NAME):$(DOCKER_IMAGE_LABEL) $(DOCKER_REPO)/$(DOCKER_IMAGE_NAME):$(DEPLOY_DOCKER_LABEL)
	docker push $(DOCKER_REPO)/$(DOCKER_IMAGE_NAME):$(DEPLOY_DOCKER_LABEL)

.PHONY: travis_setup
travis_setup: ## Setup the travis environmnet
	@if [[ -n "$$BUILD_ENV" ]] && [[ "$$BUILD_ENV" == "testing" ]]; then echo -e "$(INFO_COLOR)THIS IS EXECUTING AGAINST THE TESTING ENVIRONMEMNT$(NO_COLOR)"; fi
	@echo "Downloading packer"
	curl -s https://s3.amazonaws.com/public.ionchannel.io/files/scripts/packer_setup.sh | bash
	@chmod +x packer && mkdir -p $$HOME/.local/bin && mv packer $$HOME/.local/bin
	@echo "Installing AWS cli"
	@sudo pip install awscli

.PHONY: test
test: ## Validate the template and project
	@echo "Someone should write some tests for this project"

.PHONY: help
help:  ## Show This Help
	@for line in $$(cat Makefile | grep "##" | grep -v "grep" | sed  "s/:.*##/:/g" | sed "s/\ /!/g"); do verb=$$(echo $$line | cut -d ":" -f 1); desc=$$(echo $$line | cut -d ":" -f 2 | sed "s/!/\ /g"); printf "%-30s--%s\n" "$$verb" "$$desc"; done


