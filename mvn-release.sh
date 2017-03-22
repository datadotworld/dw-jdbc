#Copyright 2016 data.world, Inc.
#
#Licensed under the Apache License, Version 2.0 (the "License");
#you may not use this file except in compliance with the
#License.
#
#You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
#
#Unless required by applicable law or agreed to in writing, software
#distributed under the License is distributed on an "AS IS" BASIS,
#WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
#implied. See the License for the specific language governing
#permissions and limitations under the License.
#
#This product includes software developed at data.world, Inc.(http://www.data.world/).

#!/bin/bash
set -o errexit -o nounset

do_release() {
    # These variables are passed as build parameters to CircleCI
    : ${MVN_RELEASE_VER}
    : ${MVN_RELEASE_TAG}
    : ${MVN_RELEASE_DEV_VER}
    : ${MVN_RELEASE_USER_EMAIL}
    : ${MVN_RELEASE_USER_NAME}

    # These are environment variables that need to be configured within CircleCI on the project
    : ${BINTRAY_USERNAME}
    : ${BINTRAY_PASSWORD}
    : ${BINTRAY_REPO_OWNER}
    : ${BINTRAY_REPO}
    : ${SONATYPE_USERNAME}
    : ${SONATYPE_PASSWORD}
    : ${CIRCLE_PROJECT_REPONAME}

    # Passphrase associated with GPG key installed at Bintray to sign files
    : ${GPG_PASSPHRASE}

    git config user.email "${MVN_RELEASE_USER_EMAIL}"
    git config user.name "${MVN_RELEASE_USER_NAME}"

    mvn -B -Dtag=${MVN_RELEASE_TAG} release:prepare \
               -DreleaseVersion=${MVN_RELEASE_VER} \
               -DdevelopmentVersion=${MVN_RELEASE_DEV_VER} \
               -DscmCommentPrefix='[maven-release-plugin] [skip ci]'

    mvn -B -s settings.xml release:perform

    mvn release:clean

    # Instruct Bintray to GPG sign the contents of this version using the private key stored there
    curl \
        --silent \
        --fail \
        --request POST \
        --user "${BINTRAY_USERNAME}:${BINTRAY_PASSWORD}" \
        --header "X-GPG-PASSPHRASE: ${GPG_PASSPHRASE}" \
    https://api.bintray.com/gpg/${BINTRAY_REPO_OWNER}/${BINTRAY_REPO}/${CIRCLE_PROJECT_REPONAME}/versions/${MVN_RELEASE_VER}

    # Instruct Bintray to publish the signature files just created
    curl \
        --silent \
        --fail \
        --request POST \
        --user "${BINTRAY_USERNAME}:${BINTRAY_PASSWORD}" \
    https://api.bintray.com/content/${BINTRAY_REPO_OWNER}/${BINTRAY_REPO}/${CIRCLE_PROJECT_REPONAME}/${MVN_RELEASE_VER}/publish

    # Instruct Bintray to sync all files in this version to Maven Central
    curl \
        --silent \
        --fail \
        --request POST \
        --header "Content-Type: application/json" \
        --user "${BINTRAY_USERNAME}:${BINTRAY_PASSWORD}" \
        --data '{
            "username": "'${SONATYPE_USERNAME}'",
            "password": "'${SONATYPE_PASSWORD}'"
        }' \
    https://api.bintray.com/maven_central_sync/${BINTRAY_REPO_OWNER}/${BINTRAY_REPO}/${CIRCLE_PROJECT_REPONAME}/versions/${MVN_RELEASE_VER}

}

#If the environment has a Maven release version set, let's do a release
if [[ -v MVN_RELEASE_VER ]]; then
  do_release
fi
