package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/dghubble/go-twitter/twitter"
	"github.com/dghubble/oauth1"
	"github.com/olivere/elastic"
)

// Config : config struct for twitter keys
type Config struct {
	APIKEY            string `json:"apiKey"`
	APISECRET         string `json:"apiSecret"`
	ACCESSTOKENKEY    string `json:"accessTokenKey"`
	ACCESSTOKENSECRET string `json:"accessTokenSecret"`
}

// Tweet : struct used for serializing/deserializing data(tweets) in Elasticsearch
type Tweet struct {
	User     string                `json:"user"`
	Message  string                `json:"message"`
	Retweets int                   `json:"retweets"`
	Image    string                `json:"image,omitempty"`
	Created  time.Time             `json:"created,omitempty"`
	Tags     []string              `json:"tags,omitempty"`
	Location string                `json:"location,omitempty"`
	Suggest  *elastic.SuggestField `json:"suggest_field,omitempty"`
	Country  string                `json:"country,omitempty"`
	Language string                `json:"language,omitempty"`
}

// Elasticsearch mapping for tweets
const mapping = `
{
	"settings":{
		"number_of_shards": 1,
		"number_of_replicas": 0
	},
	"mappings":{
		"tweet":{
			"properties":{
				"user":{
					"type":"keyword"
				},
				"message":{
					"type":"text",
					"store": true,
					"fielddata": true
				},
				"image":{
					"type":"keyword"
				},
				"created":{
					"type":"date"
				},
				"tags":{
					"type":"keyword"
				},
				"location":{
					"type":"geo_point"
				},
				"suggest_field":{
					"type":"completion"
				},
				"country":{
					"type":"text"
				},
				"language":{
					"type":"text"
				}
			}
		}
	}
}`

// LoadConfiguration : loads config from json file
func LoadConfiguration(file string) Config {
	var config Config
	configFile, errFile := os.Open(file)
	defer configFile.Close()
	if errFile != nil {
		fmt.Println("Error opening file", errFile.Error())
	}
	jsonParser := json.NewDecoder(configFile)
	errParse := jsonParser.Decode(&config)
	if errParse != nil {
		fmt.Println("Error opening file", errParse.Error())
	}
	return config
}

func addTweet(ctx context.Context, esclient *elastic.Client, tweet *twitter.Tweet, count int) {
	coordinates := ""
	if tweet.Coordinates != nil {
		// Elasticsearch expects coordinates the other way around to how twitter gives them [lat,lon] -> [lon,lat]
		coordinates = strconv.FormatFloat(tweet.Coordinates.Coordinates[1], 'f', 6, 64) + ", " + strconv.FormatFloat(tweet.Coordinates.Coordinates[0], 'f', 6, 64)
	}
	body := Tweet{User: tweet.User.ScreenName, Message: tweet.Text, Retweets: tweet.RetweetCount, Language: tweet.Lang, Location: coordinates}
	c := strconv.Itoa(count)
	_, err := esclient.Index().
		Index("twitter").
		Type("tweet").
		Id(c).
		BodyJson(body).
		Do(ctx)
	if err != nil {
		panic(err)
	}
	// fmt.Printf("Indexed tweet %s to index %s, type %s\n", put.Id, put.Index, put.Type)
}

func main() {
	configuration := LoadConfiguration("conf.json")

	config := oauth1.NewConfig(configuration.APIKEY, configuration.APISECRET)
	token := oauth1.NewToken(configuration.ACCESSTOKENKEY, configuration.ACCESSTOKENSECRET)
	httpClient := config.Client(oauth1.NoContext, token)

	ctx := context.Background()

	// Elasticsearch client
	fmt.Println("creating client")

	var esURL = flag.String("url", "http://elasticsearch:9200", "Elasticsearch connection string")

	// TODO: Replace with reconnect on fail
	// Wait for es docker container to start
	duration := time.Second * 20
	time.Sleep(duration)

	esclient, err := elastic.NewClient(elastic.SetURL(*esURL), elastic.SetSniff(false))
	if err != nil {
		panic(err)
	}

	log.Printf("Connected to %s", *esURL)

	// Ping the Elasticsearch server to get e.g. the version number
	info, code, err := esclient.Ping(*esURL).Do(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Elasticsearch returned with code %d and version %s\n", code, info.Version.Number)

	// Create an es index, if it doesn't already exist
	exists, err := esclient.IndexExists("twitter").Do(ctx)
	if err != nil {
		panic(err)
	}
	if !exists {
		// Create a new index
		createIndex, err := esclient.CreateIndex("twitter").BodyString(mapping).Do(ctx)
		if err != nil {
			panic(err)
		}
		if !createIndex.Acknowledged {
		}
	}

	// Twitter client
	twitterclient := twitter.NewClient(httpClient)
	// SwitchDemux used to handle different types of messages (tweets, DMs...)
	demux := twitter.NewSwitchDemux()

	tweetCount := 0

	// Print tweets received from stream
	demux.Tweet = func(tweet *twitter.Tweet) {
		addTweet(ctx, esclient, tweet, tweetCount)
		tweetCount++
	}

	params := &twitter.StreamSampleParams{
		StallWarnings: twitter.Bool(true),
	}

	// Get a sample stream of tweets
	stream, err := twitterclient.Streams.Sample(params)

	// Get demux to handle stream of tweets
	go demux.HandleChan(stream.Messages)

	// Wait for cancel (CTRL-C)
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	log.Println(<-ch)
	log.Println(err)

	stream.Stop()
}
