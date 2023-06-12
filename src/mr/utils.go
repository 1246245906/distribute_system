package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
)

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func ns2s(t int64) int64 {
	return t / 1e9
}

func WriteKV2File(kvs []KeyValue, file_path string) error {
	file_dir := filepath.Dir(file_path)
	tmpfile, err := ioutil.TempFile(file_dir, "tmp*")
	defer tmpfile.Close()
	if err != nil {
		return err
	}
	// tmp_file_path := filepath.Join(file_dir, tmpfile.Name())
	if err != nil {
		return errors.New("output file create faild")
	}
	enc := json.NewEncoder(tmpfile)
	for _, kv := range kvs {
		// todo : err process
		enc.Encode(&kv)
	}
	// 感觉需要加文件锁
	err = os.Rename(tmpfile.Name(), file_path)
	if err != nil {
		fmt.Print("zhc--------" + err.Error() + "\n")
	}
	return err
}

func ReadKVFromFile(file_path string) ([]KeyValue, error) {
	ofile, err := os.Open(file_path)
	kvs := []KeyValue{}
	if err != nil {
		return []KeyValue{}, errors.New("output file create faild")
	} else {
		defer ofile.Close()
		dec := json.NewDecoder(ofile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kvs = append(kvs, kv)
		}
	}
	return kvs, nil
}
