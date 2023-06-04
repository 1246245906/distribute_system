package mr

import (
	"fmt"
	"os"
)

func ns2s(t int64) int64 {
	return t / 1e9
}

func WriteKV2File(kvs []KeyValue, file_path string) {
	ofile, err := os.Create(file_path)
	if err != nil {
		fmt.Printf("output file %s create faild\n", file_path)
	} else {
		for _, kv := range kvs {
			fmt.Fprintf(ofile, "%v %v\n", kv.Key, kv.Value)
		}
		fmt.Printf("output file %s write succ!\n", file_path)
	}
}
