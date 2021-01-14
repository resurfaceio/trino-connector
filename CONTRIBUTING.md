# Contributing to resurfaceio-trino-connector
&copy; 2016-2021 Resurface Labs Inc.

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

???