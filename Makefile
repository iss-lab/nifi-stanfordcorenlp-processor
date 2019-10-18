MVN = $(shell which mvn)
export MAVEN_OPTS = -Xmx14000m

all: deps

deps:
	$(MAKE) -C models/

test:
	cd nifi-stanfordcorenlp-processors && $(MVN) exec:java -Dexec.mainClass="com.iss.nifi.processors.stanfordcorenlp.StanfordCoreNLPService"

clean:
	$(MAKE) -C models/ clean