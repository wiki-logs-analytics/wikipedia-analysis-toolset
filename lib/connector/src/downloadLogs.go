package main

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/olivere/elastic/v7"
	"gopkg.in/yaml.v2"
)

var config *JsonConfig

type JsonConfig struct {
	Elasticsearch struct {
		Enable bool   `yaml:"enable"`
		Host   string `yaml:"host"`
		Login  string `yaml:"login"`
		Pwd    string `yaml:"pwd"`
		Index  string `yaml:"index"`
	} `yaml:"elasticsearch"`
}

type WikiLog struct {
	Date   string "json:data"
	Page   string "json:page"
	Visits uint16 "json:visits"
	Region string "json:region"
}

type LogPacket struct {
	rsp   *http.Response
	year  string
	month string
	day   string
	hours string
}

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

func downloadLog(path string, client *http.Client, c chan *LogPacket, year string, month string, day string, hours string) {

	log.Printf("Downloading %s", path)

	req, err := http.NewRequest(http.MethodGet, path, nil)
	assert(err)

	rsp, err := client.Do(req)
	assert(err)

	attempts := 50
	for true {
		if rsp.StatusCode != http.StatusOK {
			log.Print(errors.New("Failed to GET gz file " + path + " status " + rsp.Status))
			time.Sleep(time.Second * 60)
			attempts = attempts - 1
		} else {
			log.Print("Succsess, attempts left ", attempts)
			break
		}

		rsp, err = client.Do(req)
		assert(err)
	}

	log.Print("Result : ", rsp.StatusCode)

	c <- &LogPacket{rsp: rsp, year: year, month: month, day: day, hours: hours}
}

func downloadLogs(year string, month string, outputPath string, c chan *LogPacket, wg *sync.WaitGroup) {
	defer wg.Done()

	defaultRoundTripper := http.DefaultTransport
	defaultTransportPtr, ok := defaultRoundTripper.(*http.Transport)
	if !ok {
		panic(fmt.Sprintf("defaultRoundTripper not an *http.Transport"))
	}
	defaultTransport := *defaultTransportPtr
	defaultTransport.MaxIdleConns = 100
	defaultTransport.MaxIdleConnsPerHost = 100

	client := &http.Client{Transport: &defaultTransport}

	url := fmt.Sprintf("https://dumps.wikimedia.org/other/pageviews/%s/%s-%s/", year, year, month)

	log.Printf("download logs from %s to %s\n", url, outputPath)

	req, err := http.NewRequest(http.MethodGet, url, nil)
	assert(err)

	rsp, err := client.Do(req)
	assert(err)

	data, err := ioutil.ReadAll(rsp.Body)
	assert(err)

	reg, err := regexp.Compile("pageviews-\\d{8}-\\d{6}\\.gz")
	assert(err)

	for _, logGz := range removeDuplicatesUnordered(reg.FindAllString(string(data), -1)) {
		day := logGz[16:17]
		hours := logGz[19:20]
		downloadLog(url+logGz, client, c, year, month, day, hours)
	}

	c <- nil
}

func unzipAndAddToES(logs *LogPacket, es *elastic.Client, wg *sync.WaitGroup) {

	defer wg.Done()
	ctx := context.Background()
	log.Print("Decompressing")
	reader, err := gzip.NewReader(logs.rsp.Body)
	assert(err)
	defer reader.Close()

	bufReader := bufio.NewReader(reader)
	i := 0
	for true {
		line, _, _ := bufReader.ReadLine()
		if len(line) <= 0 {
			break
		}

		slice := strings.Split(string(line), " ")

		if slice[0] != "ru" {
			continue
		}

		/*		if slice[0][0:1] == "ar" {
				log = WikiLog{Page = slice[3], NumberOfVisits = slice[2], UknowValue = slice[1], Region = slice[0]}
			}*/

		numberOfVisits, _ := strconv.ParseUint(slice[2], 10, 16)
		timestamp := fmt.Sprintf("%s-%s-%sT%s:00:00Z", logs.year, logs.month, logs.day, logs.hours)
		logWiki := WikiLog{Page: slice[1],
			Visits: uint16(numberOfVisits),
			Region: slice[0],
			Date:   timestamp,
		}

		jsonLog, err := json.Marshal(logWiki)
		assert(err)

		_, err = es.Index().Index(config.Elasticsearch.Index).BodyString(string(jsonLog)).Do(ctx)
		assert(err)

		i = i + 1
	}

	log.Printf("Decompressed data - %d records", i)
}

func unzipAndAddToESTh(c chan *LogPacket, wg *sync.WaitGroup, config *JsonConfig) {
	defer wg.Done()

	es, err := elastic.NewClient()
	assert(err)
	log.Print("ES Info:")
	for true {
		logs := <-c

		if logs == nil {
			break
		}

		wg.Add(1)
		go unzipAndAddToES(logs, es, wg)
	}

}

func parseConfig(configPath *string) *JsonConfig {
	config := &JsonConfig{}

	file, err := os.Open(*configPath)
	assert(err)
	defer file.Close()

	decoder := yaml.NewDecoder(file)
	decoder.Decode(&config)
	assert(err)

	return config
}

func main() {
	var wg sync.WaitGroup

	configFile := flag.String("config", "../config.yaml", "a string")
	flag.Parse()

	config = parseConfig(configFile)

	os.Mkdir("../output", os.ModePerm)
	c := make(chan *LogPacket)
	wg.Add(2)
	go downloadLogs("2020", "10", "./output/", c, &wg)
	go unzipAndAddToESTh(c, &wg, config)

	wg.Wait()
}
