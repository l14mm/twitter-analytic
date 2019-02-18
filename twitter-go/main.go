package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
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
	info, code, err := esclient.Ping("http://elasticsearch:9200").Do(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Elasticsearch returned with code %d and version %s\n", code, info.Version.Number)

	// Create an es index
	_, err = esclient.CreateIndex("tweets").Do(ctx)
	if err != nil {
		panic(err)
	}

	// Twitter client
	twitterclient := twitter.NewClient(httpClient)
	// SwitchDemux used to handle different types of messages (tweets, DMs...)
	demux := twitter.NewSwitchDemux()

	tweetCount := 0

	// Print tweets received from stream
	demux.Tweet = func(tweet *twitter.Tweet) {
		fmt.Println(tweet.Text)
		fmt.Println("tweet count:", tweetCount)
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
