#!/bin/bash

echo -e "Updating and rebuilding/repackaging citegraph JAR...\n"
git fetch && git pull && sbt clean && sbt compile && sbt package