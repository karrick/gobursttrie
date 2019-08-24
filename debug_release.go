// +build !gobursttrie_debug

package gobursttrie

// debug is a no-op for release builds
func debug(_ string, _ ...interface{}) {}
