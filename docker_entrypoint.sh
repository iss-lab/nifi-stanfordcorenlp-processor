#!/bin/bash

apk update && apk add git

mvn -DskipTests package