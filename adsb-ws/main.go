package main

import (
	"os"
	"strconv"

	"context"
	"github.com/gin-gonic/gin"
	redis "github.com/go-redis/redis/v8"
	"strings"
)

type aircraft struct {
	ICAO             string `json:"ICAO"`
	Registration     string `json:"registration"`
	ManufacturerName string `json:"manufacture"`
	Model            string `json:"model"`
	SerialNumber     string `json:"serialnumber"`
	Owner            string `json:"owner"`
	YearBuilt        string `json:"yearbuilt"`
}

var redisServer = os.Getenv("REDISSERVER")
var redisPort = os.Getenv("REDISPORT")
var ctx = context.Background()

var rdb = connectRedis()

func connectRedis() *redis.Client {
	serverAddr := redisServer + ":" + redisPort
	return redis.NewClient(&redis.Options{
		Addr:     serverAddr,
		Password: "", // no password set
		DB:       0,  // use default DB
	})
}

func aircraftHandler(c *gin.Context) {
	icao := strings.ToLower(c.Param("icao"))
	val, err := rdb.HGetAll(ctx, "icao:"+icao).Result()
	if err != nil {
		panic(err)
	}
	var ac = aircraft{
		ICAO:             icao,
		Registration:     val["registration"],
		ManufacturerName: val["manufacturername"],
		Model:            val["model"],
		SerialNumber:     val["serialnumber"],
		Owner:            val["owner"],
		YearBuilt:        val["built"],
	}
	c.JSON(200, ac)
}

func modelHandler(c *gin.Context) {
	endQS := c.DefaultQuery("end", "100")
	end, _ := strconv.ParseInt(endQS, 10, 64)
	val, err := rdb.ZRange(ctx, "models", 0, end).Result()
	if err != nil {
		panic(err)
	}
	c.JSON(200, val)
}

func ownerHandler(c *gin.Context) {
	endQS := c.DefaultQuery("end", "100")
	end, _ := strconv.ParseInt(endQS, 10, 64)
	val, err := rdb.ZRange(ctx, "owner", 0, end).Result()
	if err != nil {
		panic(err)
	}
	c.JSON(200, val)
}

func yearsHandler(c *gin.Context) {
	endQS := c.DefaultQuery("end", "100")
	end, _ := strconv.ParseInt(endQS, 10, 64)
	val, err := rdb.ZRange(ctx, "years", 0, end).Result()
	if err != nil {
		panic(err)
	}
	c.JSON(200, val)
}
func main() {
	r := gin.Default()
	r.GET("/aircraft/:icao", aircraftHandler)
	r.GET("/models", modelHandler)
	r.GET("/years", yearsHandler)
	r.GET("/owners", ownerHandler)

	r.Run()
}
