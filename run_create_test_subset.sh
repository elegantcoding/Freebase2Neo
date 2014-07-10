#!/bin/bash
zcat $1 | grep $2 | grep type.type.instance > subset.nodes.ntriple
go run create_test_subset.go $1 math.nodes.ntriple > math.relationships.ntriple
go run create_test_subset.go $1 math.relationships.ntriple > math.subset.ntriple
