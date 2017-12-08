package util

import (
	"os"
	"bufio"
	"io"
	"runtime"
	"runtime/debug"
	"bytes"
	"encoding/gob"
	"errors"
	"os/exec"
	"sort"
)

func MultiLocalFiles(cmd string, files []string, desFilePath string) (write_count int, err error)   {
	write_count = 0
	WriteLog("begin ", cmd, "calculate, files is:", files)
	length := len(files)
	if length == 0 {
		err = errors.New("empty files array")
		return
	}
	fs := make([]*os.File, 0, length)
	readers := make([]*bufio.Reader,0,length)
	for i := 0; i < length; i++ {
		var f *os.File
		f, err = os.Open(files[i])
		if err != nil {return }
		fs = append(fs, f)
		reader := bufio.NewReader(f)
		readers = append(readers, reader)
	}
	defer func() {
		for _, f := range fs {
			f.Close()
		}
	}()

	WriteLog(cmd, " operation to desFilePath:", desFilePath)
	var desFile *os.File
	desFile, err = os.Create(desFilePath)
	if err != nil {
		return
	}
	defer desFile.Close()
	w := bufio.NewWriter(desFile)
	if length == 1 {
		for {
			if write_count%10000000 == 0 && write_count != 0 {
				WriteLog("get ", write_count, " lines")
				runtime.GC()
				debug.FreeOSMemory()
			}
			str,_, read_err := readers[0].ReadLine()
			if read_err == io.EOF {
				break
			} else {
				_,err = w.WriteString(string(str) + "\n")
				if err != nil {return }
				write_count ++
			}
		}
		if write_count == 0 {
			err = errors.New(files[0] + "is empty file")
		}
	} else {
		switch cmd {
		case "and":
			write_count, err = MultiIntersect(files, readers, w)
		case "or":
			write_count, err = MultiUnion(files, readers, w)
		case "not":
			write_count, err = MultiDiff(files, readers, w)
		default:
			err = errors.New("cmd not match")
		}
		if err != nil {
			return
		}
	}
	err = w.Flush()
	if err != nil {
		return
	}
	WriteLog("after", cmd, "calculate, result number is:", write_count)
	return
}

func MultiDiff(files []string, readers []*bufio.Reader, w *bufio.Writer) (write_count int, err error) {
	length := len(readers)
	check_strings := make([]string, 0, length)
	max_strings := make([]string, 0, length)
	all_reader_status := make([]bool, 0, length)
	for i := 0; i < length; i++{
		str,_, read_err := readers[i].ReadLine()
		if read_err == io.EOF {
			WriteLog(files[i] + " is empty file")
			all_reader_status = append(all_reader_status, true)
		} else {
			check_strings = append(check_strings, string(str))
			all_reader_status = append(all_reader_status, false)
		}
	}
	_ = deepCopy(&max_strings, &check_strings)
	count := 0
	write_count = 0
	for {
		if all_reader_status[0] == true { // 差集第一个文件为空， 返回空数组
			break
		}
		if AllEndWithException(all_reader_status, 0) {
			break
		}
		if count%10000000 == 0 && count != 0 {
			WriteLog("count ", count, " times")
			runtime.GC()
			debug.FreeOSMemory()
		}
		if write_count%10000000 == 0 &&  write_count != 0 {
			WriteLog("get ", write_count, " lines")
		}
		count = count + 1
		for i, val := range check_strings {
			max_strings[i] = val
		}
		end_list := GetEndList(all_reader_status, 0)
		end_list = append(end_list, 0)
		compare_min_string, _ := FindMinIndexes(check_strings, end_list)
		if check_strings[0] == compare_min_string {
			for i := 0; i < length; i++{
				if !all_reader_status[i] {
					str,_, read_err := readers[i].ReadLine()
					if read_err == io.EOF {
						check_strings[i] = max_strings[i]
						all_reader_status[i] = true
					} else {
						check_strings[i] = string(str)
					}
				}
			}
		} else if check_strings[0] < compare_min_string {
			_,err = w.WriteString(check_strings[0] + "\n")
			if err != nil {return }
			write_count ++
			if !all_reader_status[0] {
				str,_, read_err := readers[0].ReadLine()
				if read_err == io.EOF {
					check_strings[0] = max_strings[0]
					all_reader_status[0] = true
				} else {
					check_strings[0] = string(str)
				}
			}
		} else {
			_, min_index_list := FindMinIndexes(check_strings, end_list)
			for _, index := range min_index_list {
				if !all_reader_status[index] {
					str,_, read_err := readers[index].ReadLine()
					if read_err == io.EOF {
						check_strings[index] = max_strings[index]
						all_reader_status[index] = true
					} else {
						check_strings[index] = string(str)
					}
				}
			}
		}
	}
	if !all_reader_status[0] {
		for {
			if all_reader_status[0] {
				break
			}
			if count%10000000 == 0 && count != 0 {
				WriteLog("count ", count, " times")
				runtime.GC()
				debug.FreeOSMemory()
			}
			if write_count % 10000000 == 0 && write_count != 0 {
				WriteLog("get", write_count, "lines")
			}
			_,err = w.WriteString(check_strings[0] + "\n")
			if err != nil {return }
			write_count ++
			str,_, read_err := readers[0].ReadLine()
			if read_err == io.EOF {
				all_reader_status[0] = true
			} else {
				check_strings[0] = string(str)
			}
		}
	}
	WriteLog("write count:", write_count)
	return
}

func MultiUnion(files []string, readers []*bufio.Reader,  w *bufio.Writer) (write_count int, err error) {
	length := len(readers)
	check_strings := make([]string, 0, length)
	max_strings := make([]string, 0, length)
	all_reader_status := make([]bool, 0, length)
	for i := 0; i < length; i++{
		str,_, read_err := readers[i].ReadLine()
		if read_err == io.EOF {
			WriteLog(files[i] + " is empty file")
			check_strings = append(check_strings, "")
			all_reader_status = append(all_reader_status, true)
		} else {
			check_strings = append(check_strings, string(str))
			all_reader_status = append(all_reader_status, false)
		}
	}
	_ = deepCopy(&max_strings, &check_strings)
	count := 0
	write_count = 0
	for {
		if AllEnd(all_reader_status) {
			break
		}
		if count%10000000 == 0 && count != 0 {
			WriteLog("count ", count, " times")
			runtime.GC()
			debug.FreeOSMemory()
		}
		if write_count % 10000000 == 0 && write_count != 0 {
			WriteLog("get", write_count, "lines")
		}
		count = count + 1
		for i, val := range check_strings {
			max_strings[i] = val
		}
		end_list := GetEndList(all_reader_status, -1)
		if StringsEqual(check_strings) {
			_,err = w.WriteString(check_strings[0] + "\n")
			if err != nil {return }
			write_count ++
			for i := 0; i < length; i++{
				if !all_reader_status[i] {
					str,_, read_err := readers[i].ReadLine()
					if read_err == io.EOF {
						check_strings[i] = max_strings[i]
						all_reader_status[i] = true
					} else {
						check_strings[i] = string(str)
					}
				}
			}
		} else {
			min_string, min_index_list := FindMinIndexes(check_strings, end_list)
			_,err = w.WriteString(min_string + "\n")
			if err != nil {return }
			write_count ++
			for _, index := range min_index_list {
				if !all_reader_status[index] {
					str,_, read_err := readers[index].ReadLine()
					if read_err == io.EOF {
						check_strings[index] = max_strings[index]
						all_reader_status[index] = true
					} else {
						check_strings[index] = string(str)
					}
				}
			}
		}
	}
	WriteLog("write count:", write_count)
	return
}

func MultiIntersect(files []string, readers []*bufio.Reader,  w *bufio.Writer) (write_count int, err error) {
	length := len(readers)
	check_strings := make([]string, 0, length)
	is_end := false
	for i := 0; i < length; i++{
		str,_, read_err := readers[i].ReadLine()
		if read_err == io.EOF { // 交集， 某一个文件为空，返回空数组
			WriteLog(files[i] + " is empty file")
			is_end = true
		}
		check_strings = append(check_strings, string(str))
	}
	end_list := make([]int, 0)
	count := 0
	write_count = 0
	for {
		if is_end {
			break
		}
		if count%10000000 == 0 && count != 0 {
			WriteLog("count ", count, " times")
			runtime.GC()
			debug.FreeOSMemory()
		}
		if write_count%10000000 == 0 &&  write_count != 0 {
			WriteLog("get ", write_count, " lines")
		}
		count ++
		if StringsEqual(check_strings) {
			_,err = w.WriteString(check_strings[0] + "\n")
			if err != nil {return }
			write_count ++
			for i := 0; i < length; i++{
				str,_, read_err := readers[i].ReadLine()
				if read_err == io.EOF {
					is_end = true
					break
				} else {
					check_strings[i] = string(str)
				}
			}
		} else {
			_, min_index_list := FindMinIndexes(check_strings, end_list)
			for _, index := range min_index_list {
				str,_, read_err := readers[index].ReadLine()
				if read_err == io.EOF {
					is_end = true
				} else {
					check_strings[index] = string(str)
				}
			}
		}
	}
	WriteLog("write count:", write_count)
	return
}

func StringsEqual(check_strings []string) (isEqual bool)  {
	isEqual = true
	first := check_strings[0]
	for i :=1; i < len(check_strings); i++{
		if check_strings[i] != first {
			isEqual = false
			return
		}
	}
	return
}

func AllEnd(all_readers_status []bool) (is_end bool)  {
	is_end = true
	for i := 0; i < len(all_readers_status); i++{
		if all_readers_status[i] == false {
			is_end = false
			return
		}
	}
	return
}

func AllEndWithException(all_readers_status []bool, exception int) (is_end bool)  {
	is_end = true
	for i := 1; i < len(all_readers_status); i++{
		if i != exception {
			if all_readers_status[i] == false {
				is_end = false
				return
			}
		}
	}
	return
}

func FindMinIndexes(check_strings []string, end_list []int) (min_string string, index_list []int)  {
	resident_strings := make([]string, 0)
	for i := 0; i < len(check_strings); i++ {
		if !IntInSlice(i, end_list) {
			resident_strings = append(resident_strings, check_strings[i])
		}
	}
	min_string = resident_strings[0]
	index_list = make([]int, 0)
	if len(check_strings) == 1 {
		return
	}
	for i := 1; i < len(resident_strings); i++ {
		if resident_strings[i] < min_string {
			min_string = resident_strings[i]
		}
	}
	for i := 0; i < len(check_strings); i++ {
		if check_strings[i] == min_string {
			index_list = append(index_list, i)
		}
	}
	return
}

func GetEndList(reader_status []bool, exception int) (end_list []int) {
	end_list = make([]int, 0)
	for idx, val := range reader_status {
		if idx != exception {
			if val == true {
				end_list = append(end_list, idx)
			}
		}
	}
	return
}

func IntInSlice(a int, list []int) bool {
	for _, val := range list {
		if a == val {
			return true
		}
	}
	return false
}

func deepCopy(dst, src interface{}) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(src); err != nil {
		return err
	}
	return gob.NewDecoder(bytes.NewBuffer(buf.Bytes())).Decode(dst)
}

func WriteSliceToFile(slice []string, file_path string) (num int, err error)   {
	WriteLog("begin write to file:", file_path)
	file, err := os.Create(file_path)
	if err != nil {
		return
	}
	defer file.Close()
	w := bufio.NewWriter(file)
	write_count := 0
	for i := 0; i < len(slice); i = i + 1 {
		var str string
		str = slice[i] + "\n"
		_, err = w.WriteString(str)
		if err != nil{
			WriteLog(err)
			continue
		}
		if write_count%10000000 == 0 && write_count != 0 {
			WriteLog("write file ", file_path, " ", write_count, " lines")
			// gc immediately
			runtime.GC()
			debug.FreeOSMemory()
		}
		write_count = write_count + 1
	}
	err = w.Flush()
	if err != nil {
		return
	}
	WriteLog("write to file success:", file_path)
	WriteLog("write count:", write_count, "len(slice):", len(slice))
	num = write_count
	return
}

func CheckHadoopFiles(files []string) (err error) {
	length := len(files)
	for i := 0; i < length; i++ {
		WriteLog("checking if hadoop file is ordered:", files[i])
		WriteLog("/usr/local/hadoop/tdwgaia_ugi_ss/bin/hadoop ", " fs", " -cat ", files[i]+"/*")
		cmd1 := exec.Command("/usr/local/hadoop/tdwgaia_ugi_ss/bin/hadoop", "fs", "-cat", files[i]+"/*")
		out, out_err := cmd1.StdoutPipe()
		if err != nil {
			err = out_err
			WriteLog("Cat File", files[i]+"/*", ",Stdout Err:", err.Error())
			return
		}
		if cat_err := cmd1.Start(); err != nil {
			err = cat_err
			WriteLog("Cat File", files[i]+"/*", ",Start Error:", err.Error())
			return
		}
		WriteLog(files[i], " cat success")
		reader := bufio.NewReader(out)
		is_ordered := CheckIsOrdered(reader)
		if !is_ordered {
			err = errors.New(files[i] + " is not ordered")
			return
		}
		WriteLog("checked ok")
	}
	return
}

func CheckIsOrdered(reader *bufio.Reader) (is_ordered bool) {
	is_ordered = true
	limit := 100
	count := 0
	check_slice := make([]string ,0, limit)
	compare_slice := make([]string, 0, limit)
	for count < limit {
		str,_, read_err := reader.ReadLine()
		if read_err == io.EOF {
			break
		} else {
			check_slice = append(check_slice, string(str))
			count ++
		}
	}
	_ = deepCopy(&compare_slice, &check_slice)
	sort.Strings(compare_slice)
	for i := 0; i < len(check_slice); i++ {
		if check_slice[i] != compare_slice[i] {
			is_ordered = false
			break
		}
	}
	check_slice = make([]string, 0)
	compare_slice = make([]string, 0)
	return
}