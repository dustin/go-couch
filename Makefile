# Copyright 2009 The Go Authors. All rights reserved.
# Use of this source code is governed by a BSD-style
# license that can be found in the LICENSE file.

include $(GOROOT)/src/Make.inc

TARG=couch-go.googlecode.com/hg

GOFILES=\
	couch.go\

include $(GOROOT)/src/Make.pkg

format:
	gofmt -w couch.go
	gofmt -w couch_test.go

