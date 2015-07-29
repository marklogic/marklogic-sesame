#!/bin/bash

if [ "$TRAVIS_PULL_REQUEST" == "false" ]; then
  echo -e "Starting to update gh-pages\n"

  #go to home and setup git
  cd $HOME
  git config --global user.email "travis@travis-ci.org"
  git config --global user.name "Travis"

  #using token clone gh-pages branch
  git clone --quiet --branch=gh-pages https://${GH_TOKEN}@github.com/xquery/marklogic-sesame.git  gh-pages > /dev/null

  #go into diractory and copy data we're interested in to that directory
  cd gh-pages
  cp -Rf $HOME/build/docs ./marklogic-sesame/build/docs
  cp -Rf $HOME/build/test-results ./marklogic-sesame/build/test-results
  cp -Rf $HOME/build/reports ./marklogic-sesame/build/reports

  #add, commit and push files
  git add -f ./build
  git commit -m "Travis build $TRAVIS_BUILD_NUMBER pushed to gh-pages"
  git push -fq origin gh-pages > /dev/null

  echo -e "magic deployment to http://xquery.github.io/marklogic-sesame/javadocs\n"
fi
