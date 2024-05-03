package main

import (
	"6.5840/mr"
	"fmt"
	"log"
	"os"
	"plugin"
)

//
// start a worker process, which is implemented
// in ../mr/worker.go. typically there will be
// multiple worker processes, talking to one coordinator.
//
// go run mrworker.go wc.so
//
// Please do not change this file.
//

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrworker xxx.so\n")
		os.Exit(1)
	}
	mapf, reducef := loadPlugin(os.Args[1])
	mr.Worker(mapf, reducef)

	// local test
	//mapf := func(filename string, contents string) []mr.KeyValue {
	//	// function to detect word separators.
	//	ff := func(r rune) bool { return !unicode.IsLetter(r) }
	//
	//	// split contents into an array of words.
	//	words := strings.FieldsFunc(contents, ff)
	//
	//	kva := []mr.KeyValue{}
	//	for _, w := range words {
	//		kv := mr.KeyValue{w, "1"}
	//		kva = append(kva, kv)
	//	}
	//	return kva
	//}
	//
	//reducef := func(key string, values []string) string {
	//	// return the number of occurrences of this word.
	//	return strconv.Itoa(len(values))
	//}
	//
	//mr.LocalWorker(mapf, reducef)
	//mr.Worker(mapf, reducef)

	//mr.LocalWorker(Map, Reduce)
	//mr.Worker(Map, Reduce)

}

// load the application Map and Reduce functions
// from a plugin file, e.g. ../mrapps/wc.so
func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []mr.KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}

//func maybeCrash() {
//	max := big.NewInt(1000)
//	rr, _ := crand.Int(crand.Reader, max)
//	if rr.Int64() < 330 {
//		// crash!
//		fmt.Println("[crash] detect")
//		os.Exit(1)
//	} else if rr.Int64() < 660 {
//		// delay for a while.
//		maxms := big.NewInt(10 * 1000)
//		ms, _ := crand.Int(crand.Reader, maxms)
//		time.Sleep(time.Duration(ms.Int64()) * time.Millisecond)
//	}
//}
//
//func Map(filename string, contents string) []mr.KeyValue {
//	maybeCrash()
//
//	kva := []mr.KeyValue{}
//	kva = append(kva, mr.KeyValue{"a", filename})
//	kva = append(kva, mr.KeyValue{"b", strconv.Itoa(len(filename))})
//	kva = append(kva, mr.KeyValue{"c", strconv.Itoa(len(contents))})
//	kva = append(kva, mr.KeyValue{"d", "xyzzy"})
//	return kva
//}
//
//func Reduce(key string, values []string) string {
//	maybeCrash()
//
//	// sort values to ensure deterministic output.
//	vv := make([]string, len(values))
//	copy(vv, values)
//	sort.Strings(vv)
//
//	val := strings.Join(vv, " ")
//	return val
//}
