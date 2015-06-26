#!/bin/bash

if [ "$TRAVIS_PULL_REQUEST" == "false" ]; then
  echo -e "Starting to update gh-pages\n"

  #copy data we're interested in to other place
  cp -R dist/packages.xml $HOME/packages.xml

  #go to home and setup git
  cd $HOME
  git config --global user.email "travis@travis-ci.org"
  git config --global user.name "Travis"

  #using token clone gh-pages branch
  git clone --quiet --branch=gh-pages https://${GH_TOKEN}@github.com/xquery/marklogic-sesame.git  gh-pages > /dev/null

  #go into diractory and copy data we're interested in to that directory
  cd gh-pages
  cp -Rf $HOME/build/docs ./build/docs
  cp -Rf $HOME/build/test-results ./build/test-results
  cp -Rf $HOME/build/reports ./build/reports

  #add, commit and push files
  git add -f ./build
  git commit -m "Travis build $TRAVIS_BUILD_NUMBER pushed to gh-pages"
  git push -fq origin gh-pages > /dev/null

  echo -e "magic deployment to http://xquery.github.io/marklogic-sesame/javadocs\n"
fi
