package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/karrick/gobursttrie"
	"github.com/karrick/golf"
)

func main() {
	optCaseInsensitive := golf.Bool("i", false, "perform case insensitive search")
	optDump := golf.Bool("d", false, "dump sorted output")
	optScan := golf.Bool("s", false, "scan trie")
	golf.Parse()

	// build a new Trie from standard input lines
	trie := gobursttrie.NewStrings()

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		for _, field := range strings.Fields(scanner.Text()) {
			if *optCaseInsensitive {
				trie.Insert(strings.ToLower(field), field)
			} else {
				trie.Insert(field, struct{}{})
			}
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %s\n", err)
		os.Exit(1)
	}

	if *optDump {
		// best performance, because it maintains control flow without as much
		// function invocation back-and-forth.
		trie.Dump()
	} else if *optScan {
		// most versatile API, while having performance nearly as close to dump.
		if *optCaseInsensitive {
			for trie.Scan() {
				key, value := trie.Pair()
				fmt.Fprintf(os.Stderr, "main key: %q; value: %v\n", key, value.(string))
				// fmt.Println(value)
			}
		} else {
			for trie.Scan() {
				key := trie.Key()
				fmt.Fprintf(os.Stderr, "main key: %q\n", key)
				// fmt.Println(key)
			}
		}
	} else {
		// worst performance because this asks the trie to build a possibly
		// large list of strings, then enumerates them.
		if *optCaseInsensitive {
			for _, line := range trie.Values() {
				fmt.Println(line)
			}
		} else {
			for _, line := range trie.Keys() {
				fmt.Println(line)
			}
		}
	}
}
