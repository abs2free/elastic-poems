package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esutil"
	"github.com/siongui/gojianfan"
)

type Poem struct {
	Title      string   `json:"title"`
	Paragraphs []string `json:"paragraphs"`
	Author     string   `json:"author"`
	Rhythmic   string   `json:"rhythmic"`
	Notes      []string `json:"notes"`
}

const (
	batchSize = 1000 // 每批处理的文档数
	indexName = "poems"
)

func main() {
	// 定义命令行参数
	dirPath := flag.String("dir", ".", "Directory path containing JSON files")
	esURL := flag.String("es", "https://localhost:9200", "Elasticsearch URL")
	username := flag.String("user", "elastic", "Elasticsearch username")
	password := flag.String("pass", "123456", "Elasticsearch password")
	flag.Parse()

	cert, _ := os.ReadFile("http_ca.crt")

	// 创建 Elasticsearch 客户端
	cfg := elasticsearch.Config{
		Addresses: []string{*esURL},
		Username:  *username,
		Password:  *password,
		CACert:    cert,
	}
	client, err := elasticsearch.NewClient(cfg)
	if err != nil {
		log.Fatalf("Error creating Elasticsearch client: %s", err)
	}

	// 检查连接
	res, err := client.Info()
	if err != nil {
		log.Fatalf("Error getting Elasticsearch info: %s", err)
	}
	defer res.Body.Close()
	log.Println(res)

	// 创建批量索引器
	bulkIndexer, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Client:        client,
		Index:         indexName,
		NumWorkers:    4,
		FlushBytes:    int(2 << 20),     // 每 2MB 刷新一次
		FlushInterval: 30 * time.Second, // 每 30 秒刷新一次
	})
	if err != nil {
		log.Fatalf("Error creating bulk indexer: %s", err)
	}

	start := time.Now().UTC()

	// 遍历文件夹
	err = filepath.Walk(*dirPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() && (info.Name() == "唐诗补录.json" || info.Name() == "表面结构字.json") {
			return nil
		}

		if !info.IsDir() && filepath.Ext(path) == ".json" {
			processFile(path, bulkIndexer)
		}

		return nil
	})

	if err != nil {
		log.Fatalf("Error walking through directory: %s", err)
	}

	// 等待所有操作完成
	if err := bulkIndexer.Close(context.Background()); err != nil {
		log.Fatalf("Error closing bulk indexer: %s", err)
	}

	// <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

	biStats := bulkIndexer.Stats()

	// Report the results: number of indexed docs, number of errors, duration, indexing rate
	//

	dur := time.Since(start)

	if biStats.NumFailed > 0 {
		log.Fatalf(
			"Indexed [%s] documents with [%s] errors in %s (%s docs/sec)",
			humanize.Comma(int64(biStats.NumFlushed)),
			humanize.Comma(int64(biStats.NumFailed)),
			dur.Truncate(time.Millisecond),
			humanize.Comma(int64(1000.0/float64(dur/time.Millisecond)*float64(biStats.NumFlushed))),
		)
	} else {
		log.Printf(
			"Sucessfuly indexed [%s] documents in %s (%s docs/sec)",
			humanize.Comma(int64(biStats.NumFlushed)),
			dur.Truncate(time.Millisecond),
			humanize.Comma(int64(1000.0/float64(dur/time.Millisecond)*float64(biStats.NumFlushed))),
		)
	}

	log.Println("Indexing completed")
}

func processFile(filePath string, bulkIndexer esutil.BulkIndexer) {
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		log.Printf("Error reading file %s: %s", filePath, err)
		return
	}

	var poems []Poem
	err = json.Unmarshal(data, &poems)
	if err != nil {
		log.Printf("Error unmarshaling JSON from file %s: %s", filePath, err)
		return
	}

	// 批量添加诗歌到 Elasticsearch
	for _, poem := range poems {
		// 转换繁体到简体
		simplifiedPoem := Poem{
			Title:      gojianfan.T2S(poem.Title),
			Author:     gojianfan.T2S(poem.Author),
			Rhythmic:   gojianfan.T2S(poem.Rhythmic),
			Paragraphs: make([]string, len(poem.Paragraphs)),
			Notes:      make([]string, len(poem.Notes)),
		}

		for i, para := range poem.Paragraphs {
			simplifiedPoem.Paragraphs[i] = gojianfan.T2S(para)
		}

		for i, note := range poem.Notes {
			simplifiedPoem.Notes[i] = gojianfan.T2S(note)
		}

		// 序列化诗歌为 JSON
		doc, err := json.Marshal(simplifiedPoem)
		if err != nil {
			log.Printf("Error marshaling poem to JSON: %s", err)
			continue
		}

		// 添加到批量处理器
		err = bulkIndexer.Add(context.Background(), esutil.BulkIndexerItem{
			Action: "index",
			Body:   bytes.NewReader(doc),
			OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, resp esutil.BulkIndexerResponseItem, err error) {
				if err != nil {
					log.Printf("Error indexing document: %s", err)
				} else {
					log.Printf("Error indexing document: %s", resp.Error)
				}
			},
			// OnSuccess is the optional callback for each successful operation
			OnSuccess: func(
				ctx context.Context,
				item esutil.BulkIndexerItem,
				res esutil.BulkIndexerResponseItem,
			) {
				//fmt.Printf("[%d] %s ", res.Status, res.Result)
			},
		})
		if err != nil {
			log.Printf("Error adding document to bulk indexer: %s", err)
		}
	}

	log.Printf("Processed file: %s", filePath)
}
