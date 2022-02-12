package main

import (
	"bufio"
	"bytes"
	"context"
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
	req *fasthttp.Request
	rsp *fasthttp.Response
}

func assert(err error) {
	if err != nil {
		log.Panic(err)
	}
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

var links []string
var removeLink chan string

func generateLinks(from time.Time, to time.Time) {

	client := fasthttp.Client{
		MaxConnsPerHost: 30,
	}

	req := fasthttp.AcquireRequest()
	rsp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(rsp)

	for from.Before(to) {
		url := fmt.Sprintf("https://dumps.wikimedia.org/other/pageviews/%d/%d-%02d/", from.Year(), from.Year(), int(from.Month()))
		req.SetRequestURI(url)
		log.Printf("download logs from %s\n", url)
		client.Do(req, rsp)
		data := rsp.Body()
		reg, err := regexp.Compile("pageviews-\\d{8}-\\d{6}\\.gz")
		assert(err)
		newLinks := reg.FindAllString(string(data), -1)
		for i, newLink := range newLinks {
			newLinks[i] = url + newLink
		}
		links = append(links, newLinks...)
		from = from.Add(time.Hour * 24 * 31)
	}

	links = removeDuplicatesUnordered(links)
	sort.Strings(links)

	dumpLinksListToFile()
}

var mutex sync.Mutex

func dumpLinksListToFile() {

	mutex.Lock()
	defer mutex.Unlock()
	os.Remove("links.txt")
	f, err := os.OpenFile("links.txt", os.O_WRONLY|os.O_CREATE, 0666)
	assert(err)
	defer f.Close()

	for _, s := range links {
		_, _ = f.WriteString(s + "\n")
	}
}

func restoreLinksFromFile() {

	mutex.Lock()
	defer mutex.Unlock()

	links = nil

	f, err := os.Open("links.txt")
	assert(err)
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Split(bufio.ScanLines)

	for scanner.Scan() {
		links = append(links, scanner.Text())
	}
}

func updateLinks() {
	for true {
		doneLink := <-removeLink

		for i, link := range links {
			if link == doneLink {
				copy(links[i:], links[i+1:])
				links[len(links)-1] = ""
				links = links[:len(links)-1]
			}
		}

		dumpLinksListToFile()
	}
}

func downloadLog(path string, client *fasthttp.Client, c chan *LogPacket, count *int, mutex *sync.Mutex) {
	log.Printf("Downloading %s", path)

	defer func() {
		mutex.Lock()
		*count--
		mutex.Unlock()
	}()

	req := fasthttp.AcquireRequest()

	req.SetRequestURI(path)

	rsp := fasthttp.AcquireResponse()

	err := client.Do(req, rsp)
	for rsp.StatusCode() != 200 {
		log.Print("Failed to download ", err)
		time.Sleep(time.Second * 5)
		err = client.Do(req, rsp)
	}

	c <- &LogPacket{req: req, rsp: rsp}

}

func downloadLogs(c chan *LogPacket, wg *sync.WaitGroup) {
	defer wg.Done()

	count := 0
	mutex := sync.Mutex{}

	client := fasthttp.Client{
		MaxConnsPerHost:     30,
		MaxIdleConnDuration: 500000,
	}

	localLinks := make([]string, len(links))
	copy(localLinks, links)
	for _, link := range localLinks {
		for count >= 3 {
			time.Sleep(time.Second)
		}
		mutex.Lock()
		count++
		mutex.Unlock()
		go downloadLog(link, &client, c, &count, &mutex)
	}

	c <- nil
}

func sendToES(bulk *elastic.BulkService, wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}

	ctx := context.Background()
	cnt := 10
	for cnt > 0 {
		_, err := bulk.Do(ctx)

		if err != nil {
			log.Print(err)
			log.Print("Sleep for 5 seconds and then retry...")
			time.Sleep(time.Second * 6)
			cnt--
		} else {
			break
		}
	}
	if cnt == 0 {
		log.Print("Failed to send data")
	}
}

func parseLinkToTimestamp(link string) string {
	return fmt.Sprintf("%s-%s-%sT%s:00:00Z",
		link[44:48], link[54:56], link[73:75], link[76:78])
}

func unzipAndAddToES(logs *LogPacket, es *elastic.Client, wg *sync.WaitGroup, count *int, mutex *sync.Mutex) {

	defer wg.Done()

	defer func() {
		mutex.Lock()
		*count--
		mutex.Unlock()
	}()

	defer fasthttp.ReleaseRequest(logs.req)
	defer fasthttp.ReleaseResponse(logs.rsp)

	log.Print("Decompressing ", logs.req.URI().String())
	body, _ := logs.rsp.BodyGunzip()
	reader := bytes.NewReader(body)

	bulk := es.Bulk()
	docID := 0

	bufReader := bufio.NewReader(reader)
	russianWas := false
	for bufReader.Size() > 0 {
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

		if len(slice) < 3 {
			continue
		}

		russianWas = true

		numberOfVisits, _ := strconv.ParseUint(slice[2], 10, 16)
		doc := WikiLog{Page: slice[1],
			Visits: uint16(numberOfVisits),
			Region: slice[0],
			Date:   parseLinkToTimestamp(logs.req.URI().String()),
		}

		docID++

		req := elastic.NewBulkIndexRequest()
		req.OpType("index")
		req.Index(config.Elasticsearch.Index)
		req.Doc(doc)

		bulk.Add(req)

		if docID%50000 == 0 {

			wg.Add(1)
			sendToES(bulk, wg)

			bulk = es.Bulk()
		}
	}

	wg.Add(1)
	sendToES(bulk, wg)

	removeLink <- logs.req.URI().String()

	log.Printf("[COMPLETED] Decompressed data - %d records", docID)
}

func unzipAndAddToESTh(c chan *LogPacket, wg *sync.WaitGroup, config *JsonConfig) {
	defer wg.Done()

	count := 0
	mutex := sync.Mutex{}

	es, err := elastic.NewClient(
		elastic.SetSniff(true),
		elastic.SetURL("http://localhost:9200"),
		elastic.SetHealthcheckInterval(5*time.Second), // quit trying after 5 seconds
		elastic.SetMaxRetries(10000),
	)
	assert(err)

	log.Print("ES Info:", es)

	for true {
		logs := <-c

		if logs == nil {
			break
		}

		for count > 4 {
			time.Sleep(time.Second * 1)
		}

		mutex.Lock()
		count++
		mutex.Unlock()

		wg.Add(1)
		go unzipAndAddToES(logs, es, wg, &count, &mutex)
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
	fromDate := flag.String("startDate", "", "a string")
	toDate := flag.String("endDate", "", "a string")
	cont := flag.Bool("continue", false, "continue download")
	flag.Parse()

	if *cont {
		restoreLinksFromFile()
	} else {
		layout := "2006-01-02"
		fromDateT, err := time.Parse(layout, *fromDate)
		assert(err)
		toDateT, err := time.Parse(layout, *toDate)
		assert(err)
		generateLinks(fromDateT, toDateT)
	}

	config = parseConfig(configFile)

	os.Mkdir("../output", os.ModePerm)
	c := make(chan *LogPacket)
	removeLink = make(chan string)
	wg.Add(2)
	go downloadLogs(c, &wg)
	go unzipAndAddToESTh(c, &wg, config)
	go updateLinks()

	wg.Wait()
}
