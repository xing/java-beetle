#!/usr/bin/env bash

set -e
SNAPSHOT=$(grep -c '<version>.*-SNAPSHOT</version>' pom.xml)
VERSION = $(grep '<version>.*-SNAPSHOT</version>' pom.xml)

# Run tests in any case
.mvn clean test --settings .travis/settings.xml --batch-mode --update-snapshots

if [ "$TRAVIS_PULL_REQUEST" != "false" ]; then
  exit 0
fi


if [ "$TRAVIS_TAG" == "$TRAVIS_BRANCH" ] && [ $SNAPSHOT -eq 1 ]; then
  echo "Tag $TRAVIS_TAG with SNAPSHOT version in pom.xml: $VERSION"
  exit 1
elif [ $SNAPSHOT -ne 1 ]; then
  echo "Only build releases from tags. Got $VERSION on branch $TRAVIS_BRANCH"
  exit 0
fi

echo "Uploading to oss repo and GitHub"
./mvnw deploy --settings .travis/settings.xml -DskipTests=true --batch-mode --update-snapshots -Poss