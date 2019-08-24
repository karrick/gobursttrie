package gobursttrie

import (
	"fmt"
	"sort"
	"strings"
)

// TODO: This entire library assumes ASCII text.

// SortStrings returns a case-sensitive sorted list of strings.
func SortStrings(list []string) []string {
	trie := NewStrings()
	for _, item := range list {
		trie.Insert(item, struct{}{})
	}

	ordered := make([]string, 0, len(list))

	for trie.Scan() {
		ordered = append(ordered, trie.Key())
	}

	return ordered
}

// SortStringsCaseInsensitive returns a case-insensitive sorted list of strings.
func SortStringsCaseInsensitive(list []string) []string {
	trie := NewStrings()

	for _, item := range list {
		trie.Insert(strings.ToLower(item), item)
	}

	ordered := make([]string, 0, len(list))

	for trie.Scan() {
		v := trie.Value()
		ordered = append(ordered, v.(string))
	}

	return ordered
}

const initialBurstCapacity = 4096
const rootChildCount = 2 << 16

// Strings is the root node of a string trie.
type Strings struct {
	// globalBurstCapacity is the size used for how many key-value pairs to
	// store in new bucket nodes. When a key-value pair needs to be inserted
	// into a bucket that already has a full slice of key-value pairs, the trie
	// first checks whether this global burst capacity is larger than the
	// key-value pairs slice. When the global burst capacity is larger, then it
	// must have grown since the bucket node was created, and the bucket node is
	// grown to the new global burst capacity. Otherwise if the size of the
	// slice key-value pairs is already the same as the global burst capacity,
	// the bucket is bursted. That is, it is converted from a bucket node to an
	// internal node. This value is global only to the trie instance, but may
	// change depending on the characteristics of the keys inserted into the
	// trie.
	globalBurstCapacity int

	// children is a large array of node pointers, rather than a slice. It is
	// large because of the measured performance improvement observed by
	// splitting off the first two bytes of a key at the root node rather than
	// merely a single byte. It is an array rather than a slice because it is
	// constant in size for every trie.
	children [rootChildCount]*snode

	//
	// Scanning: The following fields are used when scanning the trie
	//

	// bookmarks is a stack of child nodes to return after this node visit is
	// complete.
	bookmarks []*sbookmark

	// rootChildIndex is the current child from the root node, tracked
	// separately because the bookmarks do not include reference from root node.
	rootChildIndex int
}

// NewStrings returns a new trie data structure used to store key-value pairs,
// where keys are strings.
func NewStrings() *Strings {
	return &Strings{globalBurstCapacity: initialBurstCapacity}
}

// Insert adds the key-value pair to the trie, allowing for duplicate keys when
// a particular key is inserted more than once. This method is in contrast to a
// Store method, which would insert a new key-value pair, or when the key is
// already in the trie, replace the value associated with that key with the new
// value.
func (trie *Strings) Insert(key string, value interface{}) {
	// Because root node has different number of children nodes, need to handle
	// routing from root slightly different than for other nodes.

	var i int
	switch len(key) {
	case 0:
		// key is the empty string, which will be stored at index 0.
	case 1:
		i = int(key[0]) << 8
		key = key[1:]
	default:
		i = int(key[0])<<8 + int(key[1])
		key = key[2:]
	}

	node := trie.children[i]
	if node == nil {
		// create bucket node
		node = &snode{pairs: make([]*spair, 0, trie.globalBurstCapacity)}
		trie.children[i] = node
	}

	for {
		if node.pairs != nil {
			// Have arrived at the leaf node.
			if l := len(node.pairs); l == cap(node.pairs) {
				// This bucket is full.
				if cap(node.pairs) == trie.globalBurstCapacity {
					// This bucket is already the max size for this
					// trie. Convert this bucket to an internal trie node.
					node.burst(trie.globalBurstCapacity)
					continue // loop again to add pair to this as a trie node.
				}
				// The global burst capacity has been raised since the last time
				// this bucket was sized. Rather than bursting now, just expand
				// the bucket to the new maximum size.
				node.growBucket(trie.globalBurstCapacity)
			}
			// There is room in this bucket for another pair.
			node.pairs = append(node.pairs, &spair{keyfragment: key, value: value})
			node.clean = false
			return
		}
		// Encountered internal node.

		if len(key) == 0 {
			i = 0 // Double up null byte to empty fragment.
		} else {
			// Strip off one byte to determine which node to visit next.
			i = int(key[0])
			key = key[1:]
		}

		child := node.children[i]
		if child == nil {
			// create bucket node
			child = &snode{pairs: make([]*spair, 0, trie.globalBurstCapacity)}
			node.children[i] = child
		}

		node = child
	}

	// NOT REACHED
}

func (trie *Strings) Dump() {
	var prefix []byte

	for i, child := range trie.children {
		if child != nil {
			b1 := byte(i >> 8)
			if b2 := byte(i & 255); b2 != 0 {
				prefix = []byte{b1, b2} // multi-byte prefix
			} else {
				prefix = []byte{b1} // single-byte prefix
			}
			child.dump(prefix)
		}
	}
}

func (node *snode) dump(prefix []byte) {
	if node.children != nil {
		for i, child := range node.children {
			if child != nil {
				child.dump(append(prefix, byte(i)))
			}
		}
		return
	}

	// bucket node
	if !node.clean {
		sort.Sort(node) // TODO: consider replacing with merge sort
		node.clean = true
	}

	for _, pair := range node.pairs {
		key := string(append(prefix, pair.keyfragment...))

		// While debugging, let's just dump the key.
		if false {
			fmt.Printf("%q: %v\n", key, pair.value)
		} else {
			fmt.Println(key)
		}
	}
}

func (trie *Strings) Keys() []string {
	var ordered []string
	var prefix []byte

	for i, child := range trie.children {
		if child != nil {
			b1 := byte(i >> 8)
			if b2 := byte(i & 255); b2 != 0 {
				prefix = []byte{b1, b2} // multi-byte prefix
			} else {
				prefix = []byte{b1} // single-byte prefix
			}
			ordered = append(ordered, child.keys(prefix)...)
		}
	}

	return ordered
}

func (node *snode) keys(prefix []byte) []string {
	var ordered []string

	if node.children != nil {
		for i, child := range node.children {
			if child != nil {
				ordered = append(ordered, child.keys(append(prefix, byte(i)))...)
			}
		}
		return ordered
	}

	// bucket node
	if !node.clean {
		sort.Sort(node) // TODO: consider replacing with merge sort
		node.clean = true
	}

	for _, pair := range node.pairs {
		ordered = append(ordered, string(append(prefix, pair.keyfragment...)))
	}

	return ordered
}

func (trie *Strings) Values() []interface{} {
	var ordered []interface{}

	for _, child := range trie.children {
		if child != nil {
			ordered = append(ordered, child.values()...)
		}
	}

	return ordered
}

func (node *snode) values() []interface{} {
	var ordered []interface{}

	if node.children != nil {
		for _, child := range node.children {
			if child != nil {
				ordered = append(ordered, child.values()...)
			}
		}
		return ordered
	}

	// bucket node
	if !node.clean {
		sort.Sort(node) // TODO: consider replacing with merge sort
		node.clean = true
	}

	for _, pair := range node.pairs {
		ordered = append(ordered, pair.value)
	}

	return ordered
}

func (trie *Strings) Scan() bool {
	// While scanning trie, when come across a bucket node, invoke its sort
	// method, prior to enumerating its contents.

	// Bookmark (node pointer, k), when run out of k, pop bookmark; when empty bookmarks, advance root index; when no more root index, return false

	// As a continuation, this function normally picks back up where it left
	// off. However, if there are no bookmarks, it has either never been
	// executed, or it has already completely enumerated the Trie's contents. In
	// either case, initialize the generator.
	ls := len(trie.bookmarks)
	if ls == 0 /* && trie.rootChildIndex == 0 */ {
		// Initialize scanner. Note the prefix is empty for very first root
		// child.
		debug("initialize scanner with first child of root\n")

		for trie.rootChildIndex = 0; trie.rootChildIndex < rootChildCount; trie.rootChildIndex++ {
			if child := trie.children[trie.rootChildIndex]; child != nil {
				// Found the first child. Add bookmark to it, then stop loop
				trie.bookmarks = append(trie.bookmarks, &sbookmark{node: child, k: -1})
				break
			}
		}

		if len(trie.bookmarks) == 0 {
			// When no bookmarks, then root has no children.
			debug("root has no children\n")
			return false
		}

		ls++
	}

	itop := ls - 1

outer:
	for {
		debug("top of outer loop: len(bookmarks): %d; itop: %d\n", len(trie.bookmarks), itop)
		if itop < 0 {
			debug("no more bookmarks for current root child node: %d\n", trie.rootChildIndex)
			// No more bookmarks for current root child node; advance to the
			// next root child.
			for ; trie.rootChildIndex < rootChildCount; trie.rootChildIndex++ {
				i := trie.rootChildIndex
				if child := trie.children[i]; child != nil {
					// Calculate child node prefix.
					var prefix []byte

					if false {
						b1 := byte(i >> 8)
						if b2 := byte(i & 255); b2 != 0 {
							prefix = []byte{b1, b2} // multi-byte prefix
						} else {
							prefix = []byte{b1} // single-byte prefix
						}
					}

					// Add bookmark for child node.
					debug("root child node: %q\n", prefix)

					trie.bookmarks = append(trie.bookmarks, &sbookmark{
						node:   child,
						prefix: prefix,
						k:      -1,
					})

					continue outer // restart loop to process newly created bookmark
				}
			}

			// Have already visited the final root child node; there is nothing
			// left to scan.
			debug("scan complete\n")
			trie.bookmarks = nil // release backing array
			trie.rootChildIndex = 0
			return false
		}

		// Load the top bookmark.
		bm := trie.bookmarks[itop]
		debug("we have at least one bookmark: %v\n", bm)

		kl := len(bm.node.pairs)
		if kl > 0 {
			bm.k++
			remaining := kl - bm.k
			if remaining == 0 {
				debug("encountered a bucket node with 0 pairs remaining\n")
				trie.bookmarks = trie.bookmarks[:itop]
				itop--
				continue
			}

			debug("encountered a bucket node with %d out of %d pairs remaining\n", remaining, kl)
			// When node is a bucket node, we simply iterate through each
			// key-value pair, passing control back upstream and signal another
			// element is available.
			for ; bm.k < kl; bm.k++ {
				debug("yielding to caller: %d\n", bm.k)
				return true
			}

			if itop--; itop >= 0 {
				// Use top bookmark by popping off the stack of bookmarks.
				bm, trie.bookmarks = trie.bookmarks[itop], trie.bookmarks[:itop+1]
				debug("new current bookmark: %v\n", bm)
			}

			continue outer
		} else {
			debug("encountered an internal node with %d pairs\n", kl)
			// When the node is an internal node, iterate through children by
			// pushing a bookmark for each onto the bookmark stack, keeping in
			// mind that not all children slice elements are valid.
			//
			// NOTE: Because only a depth-first traversal will enumerate items
			// in ascending order, pass control back upstream each time we find
			// a valid child node.

			// Look for the next child node from bookmarked node, starting at
			// previous byte from the key.
			debug("scan for next child node from bookmark\n")
			for bm.k++; bm.k < 256; bm.k++ {
				if child := bm.node.children[bm.k]; child != nil {
					prefix := append(bm.prefix, byte(bm.k))
					debug("adding bookmark to: %q\n", prefix)
					trie.bookmarks = append(trie.bookmarks, &sbookmark{
						node:   child,
						prefix: prefix,
						k:      -1,
					})
					itop++
					continue outer
				}
			}
			// the current bookmarked node has been scanned

			// Current bookmarked node has no additional children, so pop bookmark
			// stack until we find a bookmarked node that has more children to scan.
			for bm.k == 256 {
				debug("current bookmark has no additional children\n")
				if itop--; itop == -1 {
					// When the slice of bookmarks is empty, advance to next root
					// child.
					continue outer
				}

				// Use top bookmark by popping off the stack of bookmarks.
				bm, trie.bookmarks = trie.bookmarks[itop], trie.bookmarks[:itop+1]
				debug("new current bookmark: %v\n", bm)
			}
		}
	}

	// NOT REACHED
	return false
}

// Key returns the key of the key-value pair under the scanning cursor.
func (trie *Strings) Key() string {
	var prefix []byte
	b1 := byte(trie.rootChildIndex >> 8)
	if b2 := byte(trie.rootChildIndex & 255); b2 != 0 {
		prefix = []byte{b1, b2} // multi-byte prefix
	} else {
		prefix = []byte{b1} // single-byte prefix
	}
	return string(append(prefix, trie.bookmarks[len(trie.bookmarks)-1].prefix...))
}

// Pair returns the key-value pair under the scanning cursor.
func (trie *Strings) Pair() (string, interface{}) {
	bm := trie.bookmarks[len(trie.bookmarks)-1]
	value := bm.node.pairs[bm.k].value
	// TODO: eliminate the call to Key method
	return trie.Key(), value
}

// Value returns the value of the key-value pair under the scanning cursor.
func (trie *Strings) Value() interface{} {
	bm := trie.bookmarks[len(trie.bookmarks)-1]
	return bm.node.pairs[bm.k].value
}

// sbookmark is used while enumerating a prefix trie contents during scanning.
type sbookmark struct {
	node   *snode // n points to bookmarked Trie node
	prefix []byte // prefix is the collected key bytes at this node
	k      int    // k is part of the bookmark, because it is the next byte to check
}

// snode represents either an internal node or a leaf node.
type snode struct {
	//
	// All trie nodes are instantiated as bucket nodes, where the children field
	// is nil, and the pairs field is a slice of pointers to key-value
	// tuples. When the bucket is grown beyond the trie's current burst
	// capacity, this node is converted to an internal node, by allocating a
	// slice of pointers for children of its own, and moving its key-value pairs
	// into their respective buckets. At which point, the internal node will
	// have a non-nil children field, and a nil pairs field.
	//
	// If each node stored an interface to its children, then the amount of
	// space to store a list of pointers to the children would effectively
	// double, because an interface is both a pointer to the datum type and a
	// pointer to the datum type.
	//

	// children is non-nil when this node is an internal node, pointing to a
	// slice of exactly 256 node pointers.
	children []*snode // when non-nil, this is a trie node

	// pairs is non-nil when this node is a bucket, pointing to a slice of
	// key-value pair tuples which changes as key-value pairs are inserted into
	// the trie.
	pairs []*spair

	// clean is true after a bucket is sorted, until the next key-value is
	// stored in it.
	clean bool
}

// burst converts a trie bucket to a trie node. Because it's the same data
// structure, the calling function does not need to update its pointers.
func (node *snode) burst(globalBurstCapacity int) {
	node.children = make([]*snode, 256)

	for _, pair := range node.pairs {
		// strip off byte to determine which node to visit next
		var i int
		if len(pair.keyfragment) > 0 {
			// ??? doubles up null byte to empty fragment
			i = int(pair.keyfragment[0])
			pair.keyfragment = pair.keyfragment[1:] // remove first byte from string
		}

		// Get the corresponding bucket for the first byte of the key fragment.
		bucket := node.children[i]

		if bucket == nil {
			// Create a bucket if not yet created.
			bucket = &snode{pairs: make([]*spair, 0, globalBurstCapacity)}
			node.children[i] = bucket // Save the new bucket at the proper slice index.
		}

		// Add this pair to the bucket's pairs.
		bucket.pairs = append(bucket.pairs, pair)
	}

	node.pairs = nil // this node is now a trie node
	// node.clean = false // not strictly necessary
}

func (node *snode) growBucket(need int) {
	// Optimization for when slice might be required to grow by more than double
	// its size.
	if need > cap(node.pairs)<<1 {
		t := make([]*spair, need)
		copy(t, node.pairs) // can builtin copy slice of pointers?
		return
	}
	l := len(node.pairs)
	for cap(node.pairs) < need {
		node.pairs = append(node.pairs[:cap(node.pairs)], nil)
	}
	// node.pairs = node.pairs[:cap(node.pairs)]
	node.pairs = node.pairs[:l]
}

func (node *snode) Len() int {
	return len(node.pairs)
}

func (node *snode) Less(i, j int) bool {
	return node.pairs[i].keyfragment < node.pairs[j].keyfragment
}

func (node *snode) Swap(i, j int) {
	t := node.pairs[i]
	node.pairs[i] = node.pairs[j]
	node.pairs[j] = t
}

// spair represents the fragment of a remaining key along with the respective
// value. When key-value pairs are stored in a burst trie, bytes from the key
// are removed from the left edge of the key as the storage location moves
// further from trie's the root node. Whatever portion of the key is remaining
// is stored in this structure as the key fragment.
type spair struct {
	value       interface{} // actual value from upstream
	keyfragment string      // remaining portion of key (may be empty)
}
