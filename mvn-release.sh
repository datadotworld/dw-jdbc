#!/bin/bash

check_var() {
    if [[ ! -v $1 || -z $(eval echo \$${1}) ]]; then
        echo "Missing environment variable $1 : $2"
        ((++badVars))
    fi
}

resolve_vars() {
    if [[ $badVars > 0 ]]; then
        echo "There were one or more missing build variables"
        exit 1
    fi
}

do_release() {
    check_var MVN_RELEASE_TAG
    check_var MVN_RELEASE_DEV_VER
    check_var MVN_RELEASE_USER_EMAIL
    check_var MVN_RELEASE_USER_NAME
    resolve_vars

    set -e

    git config user.email "${MVN_RELEASE_USER_EMAIL}"
    git config user.name "${MVN_RELEASE_USER_NAME}"

    mvn -B -Dtag=${MVN_RELEASE_TAG} release:prepare \
               -DreleaseVersion=${MVN_RELEASE_VER} \
               -DdevelopmentVersion=${MVN_RELEASE_DEV_VER}

    mvn -B -s settings.xml release:perform

    mvn release:clean

    # Instruct Bintray to GPG sign the contents of this version using the private key stored there
    curl \
        --request POST \
        --user "${BINTRAY_USERNAME}:${BINTRAY_PASSWORD}" \
        --header "X-GPG-PASSPHRASE: ${GPG_PASSPHRASE}" \
    https://api.bintray.com/gpg/${BINTRAY_REPO_OWNER}/${BINTRAY_REPO}/${CIRCLE_PROJECT_REPONAME}/versions/${MVN_RELEASE_VER}

    # Instruct Bintray to publish the signature files just created
    curl \
        --request POST \
        --user "${BINTRAY_USERNAME}:${BINTRAY_PASSWORD}" \
    https://api.bintray.com/content/${BINTRAY_REPO_OWNER}/${BINTRAY_REPO}/${CIRCLE_PROJECT_REPONAME}/${MVN_RELEASE_VER}/publish

    # Instruct Bintray to sync all files in this version to Maven Central
    curl \
        --request POST \
        --header "Content-Type: application/json" \
        --user "${BINTRAY_USER}:${BINTRAY_PASSWORD}" \
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
