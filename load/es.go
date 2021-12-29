package load

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"reflect"
	"time"

	"github.com/olivere/elastic/v7"
)

var esClient *elastic.Client

const INDEX = "torrent"

func init() {
	// var err error
	// esClient, err = elastic.NewClient(
	// 	elastic.SetURL(config.Conf.ElasticUrl),
	// 	elastic.SetBasicAuth(config.Conf.ElasticName, config.Conf.ElasticPwd),
	// 	//elastic.SetErrorLog(os.Stdout),
	// 	elastic.SetSniff(false),
	// )
	// if err != nil {
	// 	fmt.Println("open elastic err:", err.Error())
	// 	return
	// }
}

func InsertToEs(t *Torrent) {
	codeT, err := json.Marshal(t)
	if err != nil {
		fmt.Println("encode json err ", err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	index := fmt.Sprintf("torrent-%s", time.Now().Add(-24*time.Hour).Format("20060102"))

	_, err = esClient.Index().Index(index).Id(t.HashHex).BodyString(string(codeT)).Do(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}
}

func InsertToFile(t *Torrent) {
	codeT, err := json.Marshal(t)
	if err != nil {
		fmt.Println("encode json err ", err.Error())
		return
	}

	index := fmt.Sprintf("torrent-%s", time.Now().Add(-24*time.Hour).Format("20060102"))

	torrentData := fmt.Sprintf("%s:\r\n%s\r\n%s\r\n", index, t.HashHex, string(codeT))

	logPath := time.Now().Format("2006-01-02") + ".log"
	f, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)

	if err != nil {
		return
	}

	fmt.Println(torrentData)

	_, err = io.WriteString(f, torrentData)

	defer f.Close()
}

func GetHashInfo(name string) {
	//短语搜索 搜索about字段中有 name
	matchQuery := elastic.NewMatchQuery("name", name)

	nestedQuery := elastic.NewMatchQuery("files.file_name", name)
	nestedQ := elastic.NewNestedQuery("files", nestedQuery)

	searchQuery := elastic.NewBoolQuery().Should(matchQuery, nestedQ)
	res, err := esClient.Search(INDEX).Query(searchQuery).Do(context.Background())
	if err != nil {
		fmt.Println("search err: ", err.Error())
		return
	}
	var typ Torrent
	for _, item := range res.Each(reflect.TypeOf(typ)) { //从搜索结果中取数据的方法
		t := item.(Torrent)
		fmt.Printf("%#v\n", t)
	}
}
