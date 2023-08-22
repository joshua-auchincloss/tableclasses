#!/bin/bash

COVERAGE_RCFILE=pyproject.toml
scripts/test.sh
coverage combine
coverage report --format=markdown > COVERAGE.md
coverage html
