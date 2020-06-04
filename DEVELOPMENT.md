# Development

For developing java-beetle and running the tests two things are requires:

* a JDK installation (11+)
* a docker/docker-compose installation

## Releasing

There are two possibilities to prepare a release:

* `mnv release:prepare` and `mvn:release-perform`
* manually updating version and having CI build the release (internal)

### Prepare the release

No matter which approach is used you should update and commit the Changelog for
the upcoming release version.

### Using mvn release plugin

This approach works quite well with little room for errors.

The only caveat is that this can take very long on Mac because the test suite
heavily relies on docker and the performance on OSX is not optimal
(Integration suite runtimes 30+ mins instead of 4-5 mins).

You need to have the credentials for deploying to repositories locally.

### Manual release preparation and CI

First, check that the current build of the master branch passes the full
test suite.

Then execute these steps in order:

1. Update and commit the changelog.
2. Change the version to the next release version using `mvn versions:set`
   (consistently updates parent and submodule versions)
3. If the update looks good, finalize the version update using
   `mvn versions:commit`, if not undo with `mvn versions:revert`
4. Tag the version commit with `beetle-<RELEASE VERSION>` and push the
   changes/tag (triggers release build on the internal CI)
5. Repeat steps two and three with the next `x.y-SNAPSHOT` version and
   push the change to move 'master' to the next snapshot version
