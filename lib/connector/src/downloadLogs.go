package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/olivere/elastic/v7"
	"github.com/valyala/fasthttp"
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
	rsp   *fasthttp.Response
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

func downloadLog(path string, client *fasthttp.Client, c chan *LogPacket, year string, month string, day string, hours string) {

	log.Printf("Downloading %s", path)

	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)

	req.SetRequestURI(path)

	rsp := fasthttp.AcquireResponse()

	err := client.Do(req, rsp)
	assert(err)

	attempts := 1000
	for true {
		if rsp.StatusCode() != fasthttp.StatusOK {
			log.Print(errors.New("Failed to GET gz file " + path + " status " + string(rsp.StatusCode())))
			time.Sleep(time.Second * 5)
			attempts = attempts - 1
		} else {
			log.Print("Succsess, attempts left ", attempts)
			break
		}

		err := client.Do(req, rsp)
		assert(err)
	}

	log.Print("Result : ", rsp.StatusCode())

	c <- &LogPacket{rsp: rsp, year: year, month: month, day: day, hours: hours}
}

func downloadLogs(year string, month string, outputPath string, c chan *LogPacket, wg *sync.WaitGroup) {
	defer wg.Done()

	client := fasthttp.Client{
		MaxConnsPerHost: 4,
	}

	req := fasthttp.AcquireRequest()
	rsp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(rsp)

	url := fmt.Sprintf("https://dumps.wikimedia.org/other/pageviews/%s/%s-%s/", year, year, month)

	req.SetRequestURI(url)

	log.Printf("download logs from %s to %s\n", url, outputPath)

	client.Do(req, rsp)

	data := rsp.Body()

	reg, err := regexp.Compile("pageviews-\\d{8}-\\d{6}\\.gz")
	assert(err)

	packegs := removeDuplicatesUnordered(reg.FindAllString(string(data), -1))
	sort.Strings(packegs)
	for _, logGz := range packegs {
		day := logGz[16:18]
		hours := logGz[19:21]
		downloadLog(url+logGz, &client, c, year, month, day, hours)
	}

	c <- nil
}

func unzipAndAddToES(logs *LogPacket, es *elastic.Client, wg *sync.WaitGroup, currentWorkers *uint64) {

	defer wg.Done()
	defer func() { *currentWorkers-- }()
	defer fasthttp.ReleaseResponse(logs.rsp)

	ctx := context.Background()
	log.Print("Decompressing")
	rawReader := bytes.NewReader(logs.rsp.Body())
	reader, err := gzip.NewReader(rawReader)
	assert(err)
	defer reader.Close()

	bulk := es.Bulk()
	docID := 0

	bufReader := bufio.NewReader(reader)
	russianWas := false
	for true {
		line, _, _ := bufReader.ReadLine()
		if len(line) <= 0 {
			break
		}

		slice := strings.Split(string(line), " ")

		if slice[0] != "ru" {
			if russianWas {
				break
			}
			continue
		}

		russianWas = true

		/*
			jsonLogStr := fmt.Sprintf("{\"Date\":\"%s-%s-%sT%s:00:00Z\",\"Page\":\"%s\",\"Visits\":%s,\"Region\":\"%s\"}",
				logs.year, logs.month, logs.day, logs.hours, slice[1], slice[2], slice[0])

			if slice[0][0:1] == "ar" {
				log = WikiLog{Page = slice[3], NumberOfVisits = slice[2], UknowValue = slice[1], Region = slice[0]}
			}*/

		numberOfVisits, _ := strconv.ParseUint(slice[2], 10, 16)
		timestamp := fmt.Sprintf("%s-%s-%sT%s:00:00Z", logs.year, logs.month, logs.day, logs.hours)
		doc := WikiLog{Page: slice[1],
			Visits: uint16(numberOfVisits),
			Region: slice[0],
			Date:   timestamp,
		}

		docID++

		idStr := strconv.Itoa(docID)

		req := elastic.NewBulkIndexRequest()
		req.OpType("index")
		req.Index(config.Elasticsearch.Index)
		req.Id(idStr)
		req.Doc(doc)

		bulk.Add(req)
	}

	_, err = bulk.Do(ctx)
	assert(err)

	log.Printf("[COMPLETED] Decompressed data - %d records", docID)
}

func unzipAndAddToESTh(c chan *LogPacket, wg *sync.WaitGroup, config *JsonConfig) {
	defer wg.Done()
	var currentWorkers uint64

	currentWorkers = 0

	es, err := elastic.NewClient(
		elastic.SetSniff(true),
		elastic.SetURL("http://localhost:9200"),
		elastic.SetHealthcheckInterval(5*time.Second), // quit trying after 5 seconds
	)
	assert(err)
	log.Print("ES Info:", es)
	for true {
		logs := <-c

		if logs == nil {
			break
		}

		wg.Add(1)
		for currentWorkers > 8 {
			time.Sleep(time.Second * 5)
		}

		currentWorkers++

		go unzipAndAddToES(logs, es, wg, &currentWorkers)
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

	runtime.GOMAXPROCS(runtime.NumCPU())

	log.Print("Using CPUs ", runtime.NumCPU())

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
