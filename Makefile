MVN = $(shell which mvn)
export MAVEN_OPTS = -Xmx2g -Xms2g -XX:MaxPermSize=2g

all: deps build

build:
	$(MVN) -DskipTests compile package

deps:
	$(MAKE) -C models/

test:
	$(MVN) test

test-service:
	cd nifi-stanfordcorenlp-processors && $(MVN) exec:java -Dexec.mainClass="com.iss.nifi.processors.stanfordcorenlp.StanfordCoreNLPService"

clean:
	$(MAKE) -C models/ clean