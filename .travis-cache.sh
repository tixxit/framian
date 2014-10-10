#!/usr/bin/env bash

curl -L http://s3.amazonaws.com/pellucidanalytics-framian-ivy2-cache/ivy2-cache.tar.gz | tar xzf - -C $HOME

# always succeed: the cache is optional
exit 0
