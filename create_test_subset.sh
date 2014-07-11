#!/bin/bash
gzcat $1 | egrep "$2" | grep type.type.instance > subset.nodes.ntriple
go run create_test_subset.go $1 subset.nodes.ntriple > subset.relationships.ntriple
grep type.type.instance subset.relationships.ntriple > subset.relatednodes.ntriple
go run create_test_subset.go $1 subset.relatednodes.ntriple > subset.ntriple
