# Contributing to resurfaceio-trino-connector
&copy; 2016-2022 Resurface Labs Inc.

## Coding Conventions

Our code style is whatever IntelliJ IDEA does by default, with the exception of allowing lines up to 130 characters.
If you don't use IDEA, that's ok, but your code may get reformatted.

## Git Workflow

Initial setup:

```
git clone git@github.com:resurfaceio/trino-connector.git resurfaceio-trino-connector
cd resurfaceio-trino-connector
```

Test and package:

```
mvn package
```

Committing changes:

```
git add -A
git commit -m "#123 Updated readme"       (123 is the GitHub issue number)
git pull --rebase                         (avoid merge bubbles)
git push origin master
```

Check if any newer dependencies are available:

```
mvn versions:display-dependency-updates
```

## Release Process

Push artifacts to Cloudsmith:

```bash
bash deploy.sh 3.0.(BUILD_NUMBER)
```

Tag release version:

```bash
git tag v3.0.(BUILD_NUMBER)
git push origin v3.0.x --tags
```

Start the next version by incrementing the version number. (search and replace)
