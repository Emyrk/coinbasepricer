package main

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"go.uber.org/ratelimit"
)

var rateLimiter = ratelimit.New(2)

var GDAX_API_URL = "https://api.gdax.com/"
var _ = http.NewRequest

func main() {
	fmt.Println("Parsing, should take about 1second per line. Sorry, Coinbase rate limite =/")
	var (
		from = flag.String("f", "history.csv", "Csv to read from")
		to   = flag.String("t", "modified.csv", "Csv to write to")
	)

	f, err := os.OpenFile(*from, os.O_CREATE|os.O_RDWR, 0777)
	if err != nil {
		panic(err)
	}

	os.Remove("modified.csv")

	wf, err := os.OpenFile(*to, os.O_CREATE|os.O_RDWR, 0777)
	if err != nil {
		panic(err)
	}

	defer wf.Close()
	defer f.Close()

	r := csv.NewReader(f)
	lines, err := r.ReadAll()
	if err != nil {
		panic(err)
	}

	w := csv.NewWriter(wf)
	total := len(lines)
	for i, rec := range lines {
		if i%10 == 0 {
			fmt.Printf("Completed %d/%d\n", i, total)
		}
		if i == 0 {
			extra := []string{
				"usd-amount",
				"usd-price",
				"price-date",
			}
			w.Write(append(rec, extra...))
			continue
		}
		err := w.Write(parseRecord(rec))
		if err != nil {
			panic(err)
		}
	}
	w.Flush()
}

var requestlayout = "2006-01-02T15:04:05"

func GetChartRawData(pair string, t time.Time) ([][]json.RawMessage, error) {
	var resp [][]json.RawMessage

	// Periods can be 300, 900, 1800, 7200, 14400, and 86400 seconds
	/// products/<product-id>/candles
	path := fmt.Sprintf("%s/products/%s/candles?start=%s&end=%s", GDAX_API_URL, pair, t.UTC().Format(requestlayout), t.Add(time.Hour).UTC().Format(requestlayout))

	rateLimiter.Take()
	err, _ := SendHTTPGetRequest(path, true, &resp)

	if err != nil {
		return resp, err
	}
	return resp, nil
}

func parseRecord(record []string) []string {
	t := record[1]
	v := record[2]
	coin := record[4]

	value, err := strconv.ParseFloat(v, 64)
	if err != nil {
		panic(err)
	}

	ti := parsetime(t)
	if ti.IsZero() {
		panic("Why is time 0")
	}

	var resp [][]json.RawMessage
	for {
		pair := fmt.Sprintf("%s-USD", strings.ToUpper(coin))
		resp, err = GetChartRawData(pair, ti)
		if err != nil {
			if strings.Contains(err.Error(), "Rate limit") {
				time.Sleep(3 * time.Second)
				continue
			}
			panic(err)
		}
		break
	}

	candles := RawChartstoBasicCandles(resp)
	if len(candles) == 0 {
		panic("Need at least 1 price")
	}

	thecandle := choosecandle(candles, ti)
	price := thecandle.Close
	candletime := time.Unix(thecandle.Date, 0)

	added := []string{
		fmt.Sprintf("%f", value*price),
		fmt.Sprintf("%f", price),
		fmt.Sprintf("%s", candletime.UTC().Format(requestlayout)),
	}

	newRecord := append(record, added...)
	return newRecord
}

func choosecandle(candles []BasicCandle, near time.Time) BasicCandle {
	return candles[0]
	for _, c := range candles {
		if !time.Unix(c.Date, 0).Before(near) {
			return c
		}
	}
	return candles[0]
}

func parsetime(t string) time.Time {
	var err error
	var parsedTime time.Time

	layouts := []string{
		"2006-01-02 15:04:05+00",
		"2006-01-02T15:04:05.999999Z",

		"2006-01-02 15:04:05.999999",
		"2006-01-02T15:04:05Z",
		"2006-01-02 15:04:05.999999+00"}
	for _, layout := range layouts {
		parsedTime, err = time.Parse(layout,
			strings.Replace(t, "\"", "", -1))
		if err != nil {
			continue
		}

		break
	}

	return parsedTime
}

func SendHTTPGetRequest(url string, jsonDecode bool, result interface{}) (err error, contents []byte) {
	res, err := http.Get(url)

	if err != nil {
		return err, nil
	}

	contents, err = ioutil.ReadAll(res.Body)

	if res.StatusCode != 200 {
		// log.WithFields(log.Fields{"package": "exchange", "code": res.StatusCode, "url": url}).Errorf("Error in GET api call: %s", string(contents))
		// log.Printf("HTTP status code: %d\n", res.StatusCode)
		return errors.New("Status code was not 200: " + string(contents)), contents
	}

	if err != nil {
		return err, contents
	}

	defer res.Body.Close()

	if jsonDecode {
		err := JSONDecode(contents, &result)
		if err != nil {
			return err, contents
		}
	} else {
		result = &contents
	}

	return nil, contents
}

func JSONDecode(data []byte, to interface{}) error {
	err := json.Unmarshal(data, &to)

	if err != nil {
		return err
	}

	return nil
}

type BasicCandle struct {
	Close  float64
	Volume float64
	Date   int64
}

func RawChartstoBasicCandles(raw [][]json.RawMessage) []BasicCandle {
	candles := make([]BasicCandle, 0)
	for i := range raw {
		var candle BasicCandle
		ts, err := strconv.ParseInt(string(raw[i][0]), 10, 64)
		candle.Date = ts
		candle.Volume, err = strconv.ParseFloat(string(raw[i][5]), 64)
		if err != nil {
			continue
		}
		candle.Close, err = strconv.ParseFloat(string(raw[i][4]), 64)
		if err != nil {
			continue
		}
		candles = append(candles, candle)
	}
	return candles
}

func StringSatoshiFloatToInt64(str string) (int64, error) {
	parts := strings.Split(str, ".")
	if len(parts) > 2 {
		return 0, fmt.Errorf("Invalid number: %s", str)
	}

	if len(parts) == 1 {
		parts = append(parts, "")
	}
	ap := 8 - len(parts[1])
	for i := 0; i < ap; i++ {
		parts[1] += "0"
	}
	return strconv.ParseInt(parts[0]+parts[1][:8], 10, 64)
}
