package main

import (
	"archive/zip"
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

func main() {
	//Pass inside this function the name of a file you want to unzip and count

	fileName, err := unzip("ip_addresses.zip")
	if err != nil {
		log.Fatal(err)
	}

	start := time.Now()
	chunkSize := 1000000
	chunkFiles, err := splitFile(fileName, chunkSize)
	if err != nil {
		log.Fatal(err)
	}

	uniqueCount, err := mergeChunks(chunkFiles)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Number of unique lines:", uniqueCount)

	fmt.Println("Time since start ", time.Since(start))

	for _, chunkFile := range chunkFiles {
		err = os.Remove(chunkFile)
		if err != nil {
			fmt.Println("Got error when removing chunks")
		}
	}

	err = os.Remove(fileName)
	if err != nil {
		fmt.Println("Got error when removing original file")
	}

}

func splitFile(filePath string, chunkSize int) ([]string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer func(file *os.File) {
		err = file.Close()
		if err != nil {
			fmt.Println("Got error when closing file ", err)
		}
	}(file)

	var chunkFiles []string
	buffer := make([]string, 0, chunkSize)
	scanner := bufio.NewScanner(file)
	chunkIdx := 0

	for scanner.Scan() {
		buffer = append(buffer, strings.TrimSpace(scanner.Text()))
		if len(buffer) >= chunkSize {
			chunkFile, err := writeChunk(buffer, chunkIdx)
			if err != nil {
				fmt.Println("Got error when writing chunk ", err)
				return nil, err
			}

			chunkFiles = append(chunkFiles, chunkFile)
			buffer = buffer[:0]
			chunkIdx++
		}
	}

	if len(buffer) > 0 {
		chunkFile, err := writeChunk(buffer, chunkIdx)
		if err != nil {
			fmt.Println("Got error when writing chunk ", err)
			return nil, err
		}

		chunkFiles = append(chunkFiles, chunkFile)
	}

	return chunkFiles, scanner.Err()
}

func writeChunk(lines []string, idx int) (string, error) {
	sort.Strings(lines)
	chunkFileName := fmt.Sprintf("chunk_%d.txt", idx)
	chunkFile, err := os.Create(chunkFileName)
	if err != nil {
		return "", err
	}
	defer func(chunkFile *os.File) {
		err := chunkFile.Close()
		if err != nil {
			fmt.Println("Got error when closing chunk ", err)
		}
	}(chunkFile)

	writer := bufio.NewWriter(chunkFile)
	for _, line := range lines {
		_, err := writer.WriteString(line + "\n")
		if err != nil {
			return "", err
		}
	}
	err = writer.Flush()
	if err != nil {
		fmt.Println("Got error when flushing ", err)
		return "", err
	}

	return chunkFileName, nil
}

func mergeChunks(chunkFiles []string) (int, error) {
	var files []*os.File
	for _, chunkFile := range chunkFiles {
		file, err := os.Open(chunkFile)
		if err != nil {
			return 0, err
		}
		defer func(file *os.File) {
			err = file.Close()
			if err != nil {
				fmt.Println("Got error when closing file ", err)
			}
		}(file)
		files = append(files, file)
	}

	uniqueCount := 0
	var lastLine string

	readers := make([]*bufio.Scanner, len(files))
	for i, file := range files {
		readers[i] = bufio.NewScanner(file)
	}

	lines := make([]string, len(files))
	for i, reader := range readers {
		if reader.Scan() {
			lines[i] = reader.Text()
		} else {
			lines[i] = ""
		}
	}

	for {
		minLine := ""
		minIdx := -1
		for i, line := range lines {
			if line != "" && (minLine == "" || line < minLine) {
				minLine = line
				minIdx = i
			}
		}

		if minIdx == -1 {
			break
		}

		if minLine != lastLine {
			uniqueCount++
			lastLine = minLine
		}

		if readers[minIdx].Scan() {
			lines[minIdx] = readers[minIdx].Text()
		} else {
			lines[minIdx] = ""
		}
	}

	return uniqueCount, nil
}

func unzip(zipName string) (string, error) {
	var fileName string
	r, err := zip.OpenReader(zipName)
	if err != nil {
		fmt.Println("Error when opening zip file:", err)
		return fileName, err
	}

	defer func(r *zip.ReadCloser) {
		err = r.Close()
		if err != nil {
			fmt.Println("Error when closing zip file:", err)
		}
	}(r)

	for _, f := range r.File {
		path := filepath.Join("", f.Name)
		fileName = f.Name

		if f.FileInfo().IsDir() {
			err = os.MkdirAll(path, f.Mode())
			if err != nil {
				return fileName, err
			}
			continue
		}

		if err = os.MkdirAll(filepath.Dir(path), f.Mode()); err != nil {
			fmt.Println("Error when creating directory", err)
			return fileName, err
		}

		rc, err := f.Open()
		if err != nil {
			fmt.Println("Got error when opening file:", err)
			return fileName, err
		}

		defer func(rc io.ReadCloser) {
			err = rc.Close()
			if err != nil {
				fmt.Println("Got error when closing file:", err)
			}
		}(rc)

		outFile, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, f.Mode())
		if err != nil {
			fmt.Println("Error when creating file", err)
			return fileName, err
		}

		defer func(outFile *os.File) {
			err = outFile.Close()
			if err != nil {
				fmt.Println("Error when closing file", err)
			}
		}(outFile)

		_, err = io.Copy(outFile, rc)
		if err != nil {
			fmt.Println("Error when unzipping", err)
			return fileName, err
		}
	}
	fmt.Println("Done")
	return fileName, nil
}
