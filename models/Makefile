ENGLISH_MODEL = stanford-english-corenlp-2018-10-05-models.jar
ENGLISH_MODEL_URL = https://nlp.stanford.edu/software/${ENGLISH_MODEL}

all: ${ENGLISH_MODEL}

${ENGLISH_MODEL}:
	wget ${ENGLISH_MODEL_URL}

clean:
	-rm -f *.jar