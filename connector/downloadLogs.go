package main

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"regexp"
	"sync"
	"time"
)

var completionStr string
var numDownloaders int

func assert(err error) {
	if err != nil {
		log.Panic(err)
	}
}

func saveData(data []byte, outPath string) {
	file, err := os.OpenFile(outPath, os.O_WRONLY|os.O_CREATE, os.ModePerm)
	assert(err)
	defer file.Close()

	_, errWrite := file.Write(data)
	assert(errWrite)

	log.Printf("Saved data to %s", outPath)
}

func removeDuplicatesUnordered(elements []string) []string {
	encountered := map[string]bool{}

	for v := range elements {
		encountered[elements[v]] = true
	}

	result := []string{}
	for key := range encountered {
		result = append(result, key)
	}
	return result
}

func downloadLog(path string, c chan string, wg *sync.WaitGroup) {
	defer wg.Done()
	var rsp http.Response

	client := http.Client{
		Timeout: time.Second * 10000,
	}

	log.Printf("Downloading %s", path)

	req, err := http.NewRequest(http.MethodGet, path, nil)
	assert(err)

	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
	req.Header.Set("Accept-Encoding", "gzip, deflate, bt")
	req.Header.Set("Accept-Language", "en-US,en;q=0.5")
	req.Header.Set("Connection", "keep-alive")
	/*	req.Header.Set("User-Agent", "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:82.0) Gecko/20100101 Firefox/82.0")
		req.Header.Set("Cookie", "GeoIP=RU:LEN:Murino:60.05:30.45:v4; WMF-Last-Access-Global=28-Oct-2020")
		req.Header.Set("Host", "dumps.wikimedia.org")
		req.Header.Set("Upgrade-Insecure-Requests", "1")
		req.Header.Set("Referer", "https://dumps.wikimedia.org/other/pageviews/2020/2020-10/")*/

	for true {
		rsp, err := client.Do(req)
		assert(err)

		if rsp.StatusCode == 503 {
			log.Print("Got temporary unavailable")
			time.Sleep(time.Second * 5)
			continue
		}

		if rsp.StatusCode != http.StatusOK {
			log.Panic(errors.New("Failed to GET gz file " + path + " status " + rsp.Status))
		}

	}

	data, err := ioutil.ReadAll(rsp.Body)
	assert(err)

	saveData(data, path)

	c <- path

	numDownloaders--
}

func downloadLogs(year string, month string, outputPath string, c chan string, wg *sync.WaitGroup) {
	defer wg.Done()

	var downloadWg sync.WaitGroup

	numDownloaders = 0

	httpClient := http.Client{
		Timeout: time.Second * 10000,
	}

	url := fmt.Sprintf("https://dumps.wikimedia.org/other/pageviews/%s/%s-%s/", year, year, month)

	log.Printf("download logs from %s to %s\n", url, outputPath)

	req, err := http.NewRequest(http.MethodGet, url, nil)
	assert(err)

	rsp, err := httpClient.Do(req)
	assert(err)

	data, err := ioutil.ReadAll(rsp.Body)
	assert(err)

	reg, err := regexp.Compile("pageviews-\\d{8}-\\d{6}\\.gz")
	assert(err)

	for _, logGz := range removeDuplicatesUnordered(reg.FindAllString(string(data), -1)) {
		downloadWg.Add(1)
		numDownloaders++
		go downloadLog(url+logGz, c, &downloadWg)
		for numDownloaders >= 2 {
			time.Sleep(time.Second * 1)
		}
	}

	downloadWg.Wait()
	c <- completionStr
}

func unzipAndAddData(filePath string) {
	log.Printf("Decompressing %s", filePath)

	file, err := os.Open(filePath)
	assert(err)
	defer file.Close()

	reader, err := gzip.NewReader(file)
	assert(err)
	defer reader.Close()

	writer := bytes.NewBufferString("")

	io.Copy(writer, reader)

	log.Printf("Decompressed file %s data -  %s...", filePath, writer.String()[0:10])
}

func unzipAndAddDataTh(c chan string, wg *sync.WaitGroup) {
	defer wg.Done()

	for true {
		filePath := <-c

		if filePath == completionStr {
			break
		}

		unzipAndAddData(filePath)
	}

}

func main() {
	var wg sync.WaitGroup

	completionStr = "_COMPLETE_"

	os.Mkdir("./output", os.ModePerm)
	c := make(chan string)
	wg.Add(2)
	go downloadLogs("2020", "10", "./output/", c, &wg)
	go unzipAndAddDataTh(c, &wg)

	wg.Wait()
}
