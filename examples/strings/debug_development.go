// +build debug

package main

import (
	"fmt"
	"os"
)

// debug formats and prints arguments to stderr for development builds
func debug(f string, a ...interface{}) {
	fmt.Fprintf(os.Stderr, f, a...)
}
