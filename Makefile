MVN = $(shell which mvn)
export MAVEN_OPTS = -Xmx2g -Xms2g

all: build

docker-build:
	docker run -v ~/.m2/repository:/usr/share/maven/ref/repository -v $(PWD):/usr/src/app -w /usr/src/app maven:3.5.2-jdk-8-alpine /usr/src/app/docker_entrypoint.sh

build:
	$(MVN) -DskipTests compile package

test:
	$(MVN) test

clean:
	$(MVN) clean