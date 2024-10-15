package main

import (
	"context"
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/olivere/elastic/v7"
	"github.com/siongui/gojianfan"
)

type Poem struct {
	Title       string   `json:"title"`
	Paragraphs  []string `json:"paragraphs"`
	Author      string   `json:"author"`
	Rhythmic    string   `json:"rhythmic"`
	Notes       []string `json:"notes"`
	Name        string   `json:"name"`
	Desc        string   `json:"desc"`
	Description string   `json:"description"`
}

const (
	batchSize = 1000 // 每批处理的文档数
	indexName = "poems"
)

func main() {
	// 定义命令行参数
	dirPath := flag.String("dir", ".", "Directory path containing JSON files")
	esURL := flag.String("es", "http://localhost:9200", "Elasticsearch URL")
	username := flag.String("user", "elastic", "Elasticsearch username")
	password := flag.String("pass", "123456", "Elasticsearch password")
	flag.Parse()

	// 创建 Elasticsearch 客户端
	client, err := elastic.NewClient(
		elastic.SetURL(*esURL),
		elastic.SetBasicAuth(*username, *password),
		elastic.SetSniff(false),
	)
	if err != nil {
		log.Fatalf("Error creating Elasticsearch client: %s", err)
	}

	// 检查连接
	info, code, err := client.Ping(*esURL).Do(context.Background())
	if err != nil {
		log.Fatalf("Error pinging Elasticsearch: %s", err)
	}
	log.Printf("Elasticsearch returned with code %d and version %s\n", code, info.Version.Number)

	// 创建批量处理服务
	bulkProcessor, err := client.BulkProcessor().
		Name("MyBackgroundWorker").
		Workers(4).
		BulkActions(batchSize).          // 每 1000 个操作刷新一次
		BulkSize(2 << 20).               // 每 2MB 刷新一次
		FlushInterval(30 * time.Second). // 每 30 秒刷新一次
		Do(context.Background())
	if err != nil {
		log.Fatalf("Error creating bulk processor: %s", err)
	}
	defer bulkProcessor.Close()

	// 遍历文件夹
	err = filepath.Walk(*dirPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// 如果是 error 文件夹，则跳过该目录
		if info.IsDir() && info.Name() == "error" {
			return filepath.SkipDir
		}

		if !info.IsDir() && filepath.Ext(path) == ".json" {
			processFile(path, bulkProcessor)
		}
		return nil
	})

	if err != nil {
		log.Fatalf("Error walking through directory: %s", err)
	}

	// 等待所有操作完成
	if err := bulkProcessor.Flush(); err != nil {
		log.Fatalf("Error flushing bulk processor: %s", err)
	}

	log.Println("Indexing completed")
}

func processFile(filePath string, bulkProcessor *elastic.BulkProcessor) {
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

		req := elastic.NewBulkIndexRequest().
			Index(indexName).
			Doc(simplifiedPoem)
		bulkProcessor.Add(req)
	}

	log.Printf("Processed file: %s", filePath)
}

func singleImport() {
	// 定义命令行参数
	dirPath := flag.String("dir", ".", "Directory path containing JSON files")
	esURL := flag.String("es", "http://localhost:9200", "Elasticsearch URL")
	flag.Parse()

	// Elasticsearch 连接设置
	url := "http://localhost:9200" // 替换为您的 Elasticsearch URL
	username := "elastic"          // 替换为您的用户名
	password := "123456"           // 替换为您的密码

	// 创建 Elasticsearch 客户端
	client, err := elastic.NewClient(
		elastic.SetURL(*esURL),
		elastic.SetBasicAuth(username, password),
		elastic.SetSniff(false), // 可选：如果在 Docker 或 NAT 环境中运行，可能需要禁用 sniffing
	)
	if err != nil {
		log.Fatalf("Error creating Elasticsearch client: %s", err)
	}

	// 检查连接
	info, code, err := client.Ping(url).Do(context.Background())
	if err != nil {
		log.Fatalf("Error pinging Elasticsearch: %s", err)
	}
	log.Printf("Elasticsearch returned with code %d and version %s\n", code, info.Version.Number)

	// 遍历文件夹
	err = filepath.Walk(*dirPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && filepath.Ext(path) == ".json" {
			singleProcessFile(path, client)
		}
		return nil
	})

	if err != nil {
		log.Fatalf("Error walking through directory: %s", err)
	}

	log.Println("Indexing completed")
}

func singleProcessFile(filePath string, client *elastic.Client) {
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

	// 将诗歌数据存入 Elasticsearch
	for _, poem := range poems {
		_, err := client.Index().
			Index("poems").
			BodyJson(poem).
			Do(context.Background())

		if err != nil {
			log.Printf("Error indexing poem %s: %s", poem.Title, err)
		} else {
			log.Printf("Successfully indexed poem: %s", poem.Title)
		}
	}
}
