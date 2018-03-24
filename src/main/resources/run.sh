#!/usr/bin/env bash
java -cp $(for i in lib/*.jar ; do echo -n $i: ; done) snapp /etl/Main