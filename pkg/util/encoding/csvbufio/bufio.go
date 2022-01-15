// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package csvbufio implements buffered I/O. It wraps an io.Reader or io.Writer
// object, creating another object (Reader or Writer) that also implements
// the interface but provides buffering and some help for textual I/O.
package csvbufio

// ReadSeekCloser = Reader + Seeker + Closer
//type ReadSeekCloser interface {
//	io.Reader
//	io.Seeker
//	io.Closer
//	io.Writer
//}
