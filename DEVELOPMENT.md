# Development

For developing java-beetle and running the tests two things are required:

* a JDK installation (11+)
* a docker/docker-compose installation

## Releasing

The release process is managed by travis-ci and releases are pushed to the central Maven repository.

It uses the `org.sonatype.plugins::nexus-staging-maven-plugin` to perform release publishing as recommended by
the [OSSRH Guide](https://central.sonatype.org/pages/ossrh-guide.html) in the detail docs 
for [Maven](https://central.sonatype.org/pages/apache-maven.html).

### Release credentials

Releasing to Maven central requires credentials (username/password) on the [Sonatype JIRA](https://issues.sonatype.org/)
for the deployment process.

The user specified must have been authorized via Ticket there to get deployment
access to the groupId `com.xing.*` before being able to deploy to the staging server of Maven Central.

The process is quite simple: Ask for access to the groupId `com.xing.*` via ticket and have one of the existing users
with deployment permission approve the request via issue comment. This usually only takes a few hours.

The username and password have to be configured as `NEXUS_USERNAME` and `NEXUS_PASSWORD` environment variable at the
[Travis CI project settings][1].

Usage of both the repository credentials and the key used for signing is configured in `.travis/settings.xml`

### Prepare the release

First step is to check that the current build of the master branch passes the full test suite.

Then execute these steps in order:

1. Update and commit the changelog

2. Change the version to the next release version using `mvn versions:set`
   (consistently updates parent and submodule versions)
   
3. If the update looks good, finalize the version update using
   `mvn versions:commit`, if not undo with `mvn versions:revert`
   
4. Tag the version commit with `beetle-<RELEASE VERSION>`  

5. Push the version and tag with `git push` and `git push --tags` respectively

6. Repeat steps two and three with the next `x.y-SNAPSHOT` version and
   push the change to move 'master' to the next snapshot version


## Release signing

Maven central requires published artifacts to be gpg signed. The signing process is set up in the top-level pom.

### Key management

Setting up or changing the signing key is a n-step process:

1. Create (or extend) gpg key and upload the public key to a keyserver.
   (`gpg` usually generates a revocation certificate that *should be kept in a safe place to revoke the certificate
   in the event the key is compromised or lost*)
   
2. Export the secret key to a file and add it as encrypted file to travis

3. Set up the key passphrase as environment variable `GPG_PASSPHRASE`
   in the [Travis CI project settings][1]
   
e.g.

```shell
gpg --gen-key
# Fill in data as requested.

gpg --export-secret-keys > .travis/secret_keys.gpg
# Only export the key to use for release signing in travis!
# Cancel the passphrase dialouge for all other private keys in the private keyring

travis encrypt-file .travis/secret_keys.gpg .travis/secret_keys.gpg.enc # add '--add' for adding decrypting to .travis.yml automatically
```

[1]: https://travis-ci.org/github/xing/java-beetle/settings "Travis-CI Settings for xing/java-beetle"
