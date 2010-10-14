# Copyright 2009 The Go Authors. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

include $(GOROOT)/src/Make.inc

TARG=couch-go.googlecode.com/hg
GOFMT=gofmt -spaces=true -tabindent=false -tabwidth=4

GOFILES=\
	couch.go\

include $(GOROOT)/src/Make.pkg

format:
	${GOFMT} -w couch.go
	$(GOFMT) -w couch_test.go

