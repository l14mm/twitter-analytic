package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/dghubble/go-twitter/twitter"
	"github.com/dghubble/oauth1"
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

	// Twitter client
	client := twitter.NewClient(httpClient)
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
	stream, err := client.Streams.Sample(params)

	// Get demux to handle stream of tweets
	go demux.HandleChan(stream.Messages)

	// Wait for cancel (CTRL-C)
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	log.Println(<-ch)
	log.Println(err)

	stream.Stop()
}
