package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"strings"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	for ping() {
		doMapTask(mapf)

		// fmt.Println("Map tasks done, sleeping for 5 seconds")
		// time.Sleep(time.Second * 5)
		for {
			reply := getReduceTask()
			// print the reply.wait
			if reply.wait {
				fmt.Println("Reduce task waiting")
				time.Sleep(time.Second * 1)
				continue
			}
			if len(reply.Filenames) == 0 {
				break
			}
			// fmt.Println("Reduce task received")
			oname := "mr-out-" + fmt.Sprintf("%d", reply.ReduceN)
			ofile, _ := os.Create(oname)
			// for the files in reply.Filenames
			kvs := make(map[string][]string)
			for _, filename := range reply.Filenames {
				baseFilename := filepath.Base(filename)
				fname := "map-" + baseFilename + "-" + fmt.Sprintf("%d", reply.ReduceN)
				file, _ := os.Open(fname)
				defer file.Close()
				defer os.Remove(fname)
				scanner := bufio.NewScanner(file)
				for scanner.Scan() {
					line := scanner.Text()
					kv := strings.Split(line, " ")
					key := kv[0]
					value := kv[1]
					kvs[key] = append(kvs[key], value)
				}
			}
			for key, values := range kvs {
				output := reducef(key, values)
				fmt.Fprintf(ofile, "%v %v\n", key, output)
			}
			markReduceTaskDone(reply.ReduceN)
		}
		time.Sleep(time.Second * 1)
	}

	// When the job is completely finished, the worker processes should exit. A simple way to implement this is to use the return value from call(): if the worker fails to contact the coordinator, it can assume that the coordinator has exited because the job is done, so the worker can terminate too. Depending on your design, you might also find it helpful to have a "please exit" pseudo-task that the coordinator can give to workers.

}

func doMapTask(mapf func(string, string) []KeyValue) {
	for {
		reply := getMapTask()
		if reply.Filename == "" {
			break
		}
		contents, err := ioutil.ReadFile(reply.Filename)
		if err != nil {
			log.Fatal("reading file:", err)
		}
		// fmt.Println("Map task received")
		kv := mapf(reply.Filename, string(contents))
		// Create an output file for each reduce task
		outputFiles := make([]*os.File, reply.NReduce)
		for i := 0; i < reply.NReduce; i++ {
			baseFilename := filepath.Base(reply.Filename)
			oname := "map-" + baseFilename + "-" + fmt.Sprintf("%d", i)
			ofile, err := os.Create(oname)
			if err != nil {
				log.Fatal("creating file:", err)
			}
			defer ofile.Close()
			outputFiles[i] = ofile
		}

		// Distribute keys to the appropriate reduce task
		for _, kvPair := range kv {
			reduceTask := ihash(kvPair.Key) % reply.NReduce
			fmt.Fprintf(outputFiles[reduceTask], "%v %v\n", kvPair.Key, kvPair.Value)
		}

		markMapTaskDone(reply.Filename)
	}
}

func getMapTask() GetMapReply {
	args := GetMapArgs{}
	reply := GetMapReply{}
	call("Coordinator.GetMapTask", &args, &reply)
	return reply
}

func getReduceTask() GetReduceReply {
	args := GetReduceArgs{}
	reply := GetReduceReply{}
	call("Coordinator.GetReduceTask", &args, &reply)
	return reply
}

func markMapTaskDone(filename string) MarkMapTaskDoneReply {
	args := MarkMapTaskDoneArgs{Filename: filename}
	reply := MarkMapTaskDoneReply{}
	call("Coordinator.MarkMapTaskDone", &args, &reply)
	return reply
}

func markReduceTaskDone(reduceN int) MarkReduceTaskDoneReply {
	args := MarkReduceTaskDoneArgs{ReduceN: reduceN}
	reply := MarkReduceTaskDoneReply{}
	call("Coordinator.MarkReduceTaskDone", &args, &reply)
	return reply
}

func ping() bool {
	args := struct{}{}
	reply := struct{}{}
	success := call("Coordinator.Ping", &args, &reply)
	return success
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//

func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
