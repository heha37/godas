package dataframe_test

import (
	"encoding/json"
	"fmt"
	"github.com/hunknownz/godas/dataframe"
	se "github.com/hunknownz/godas/series"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"
)

func TestNewDataFrame(t *testing.T) {
	dataInt := []int{
		1,2,3,4,5,
	}
	dataInt2 := []int{
		6,7,8,9,10,
	}
	seriesInt := se.New(dataInt, "")
	seriesInt2 := se.New(dataInt2, "")

	df, _ := dataframe.NewFromSeries(seriesInt, seriesInt2)
	fmt.Printf("%v\n", df)
}

func TestDataFrameCondition(t *testing.T) {
	dataInt := []int{
		1,2,3,4,5,
	}
	dataInt2 := []int{
		6,7,8,9,10,
	}
	seriesInt := se.New(dataInt, "")
	seriesInt2 := se.New(dataInt2, "")

	df, _ := dataframe.NewFromSeries(seriesInt, seriesInt2)
	condition := dataframe.NewCondition()
	condition.Or(">", 2, "0")
	condition.And("<", 9, "1")
	ixs, e := df.IsCondition(condition)
	fmt.Printf("%v %v\n", ixs, e)
	newDataFrame, _ := df.Filter(condition)
	fmt.Printf("%v\n", newDataFrame)
	series, _ := newDataFrame.GetSeriesByColumn("0")
	fmt.Printf("%v\n", series)
}

func TestDataFrameLightCondition(t *testing.T) {
	dataInt := []int{
		1,3,
	}
	dataInt2 := []int{
		6,7,
	}
	seriesInt := se.New(dataInt, "")
	seriesInt2 := se.New(dataInt2, "")

	df, _ := dataframe.NewFromSeries(seriesInt, seriesInt2)
	condition := dataframe.NewCondition()
	condition.Or(">", int64(2), "0")
	condition.And("<", int64(9), "1")
	ixs, _ := df.IsCondition(condition)
	fmt.Printf("%v\n", ixs)
	newDataFrame, _ := df.Filter(condition)
	fmt.Printf("%v\n", newDataFrame)
	series, _ := newDataFrame.GetSeriesByColumn("0")
	fmt.Printf("%v\n", series)
}

// http methods
const (
	MethodGet    = "GET"
	MethodPost   = "POST"
	MethodPut    = "PUT"
	MethodPatch  = "PATCH"
	MethodDelete = "DELETE"
)

var client = &http.Client{
	Timeout: time.Duration(5)*time.Second,
}

// CallAPI calls HTTP API
func CallAPI(method, url, body string, headers map[string]string) (data []byte, err error) {
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		fmt.Printf("%v\n", err)
		return
	}
	for k, v := range headers {
		req.Header.Add(k, v)
	}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("%v\n", err)
		return
	}
	defer resp.Body.Close()
	data, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Printf("%v\n", err)
		return
	}
	if resp.StatusCode >= http.StatusBadRequest {
		fmt.Printf("%v\n", err)
		return nil, err
	}
	return
}

type MovieReq struct {
	Movie MovieItem `json:"movie"`
}

type MovieItem struct {
	ID int `json:"id"`
	Name string `json:"name"`
	DoubanSubjectID string `json:"douban_subject_id"`
	PosterURL string `json:"poster_url"`
	Description string `json:"description"`
	Actors string `json:"actors"`
	Directors string `json:"directors"`
	Genres string `json:"genres"`
	FilePath string `json:"file_path"`
}

func TestNewFromCSV(t *testing.T) {
	df, _ := dataframe.NewFromCSV("./movies.csv")
	dfLen := df.Len()
	fmt.Printf("%v\n", dfLen)
	for i := 0; i < dfLen; i++ {
		value, _ := df.At(i, "\"MOVIE_ID\"")
		doubanID := value.MustString()
		value, _ = df.At(i, "NAME")
		name := value.MustString()
		value, _ = df.At(i, "COVER")
		cover := value.MustString()
		value, _ = df.At(i, "STORYLINE")
		description := value.MustString()
		value, _ = df.At(i, "ACTORS")
		actors := value.MustString()
		value, _ = df.At(i, "DIRECTORS")
		directors := value.MustString()
		value, _ = df.At(i, "GENRES")
		genres := value.MustString()

		fmt.Printf("%v %v %v %v\n", doubanID, name, cover, description)
		movieReq := &MovieReq{
			Movie: MovieItem{
				Name: name,
				DoubanSubjectID: doubanID,
				PosterURL: cover,
				Description: description,
				Actors: actors,
				Directors: directors,
				Genres: genres,
				FilePath: "product/" + name + ".mp4",
			},
		}

		b, _ := json.Marshal(movieReq)
		data, err := CallAPI(MethodPost, "http://jinquntech.com/nas/v1/movies", string(b), map[string]string{"Content-Type": "application/json"})
		if err != nil {
			fmt.Printf("err: cnt-%d", i)
		}
		fmt.Printf("%v\n", string(data))

		movieResp := new(MovieReq)
		json.Unmarshal(data, movieResp)
		idString := strconv.Itoa(movieResp.Movie.ID)
		data, err = CallAPI(MethodPatch, "http://jinquntech.com/nas/v1/movies/"+idString, string(b), map[string]string{"Content-Type": "application/json"})
		if err != nil {
			fmt.Printf("err: cnt-%d", i)
		}
		fmt.Printf("%v\n", string(data))

		time.Sleep(time.Duration(1)*time.Second)
	}
}

func TestDataFrameAt(t *testing.T) {
	df, _ := dataframe.NewFromCSV("./jinan.csv")
	v, _ := df.At(0, "NAME")
	fmt.Printf("%v\n", v.MustString())
}

type People struct {
	gorm.Model
	Name string
	Sex string

	Units string
	Posts string
	Sources string
	Links string

	Update time.Time
	Create time.Time
}

func TestNewFromCSVOfficer(t *testing.T) {
	df, _ := dataframe.NewFromCSV("./jinan.csv")
	dfLen := df.Len()

	for i := 0; i < dfLen; i++ {
			value, _ := df.At(i, "unit")
			unit := value.MustString()

			value, _ = df.At(i, "name")
			name := value.MustString()

			value, _ = df.At(i, "sex")
			sex := value.MustString()

			people := new(People)
			people.Name = name
			if sex == "男" {
				people.Sex = "MALE"
			} else {
				people.Sex = "FEMALE"
			}
			people.Units = unit
			people.Posts = "NaN"
			people.Sources = "http://www.jnbb.gov.cn/smzgs/"
			people.Links = "http://www.jnbb.gov.cn/smzgs/"

			people.Create = time.Now()
			people.Update = time.Now()


			fmt.Printf("%v\n", people)
	}
}

func TestNewFromStructs(t *testing.T) {
	type User struct {
		Name string
		Age int
		Height float64
		Phone string
	}

	user := new(User)
	user.Name = "abc"
	user.Age = 1
	user.Height = 1.84
	user.Phone = "3423432424"

	user1 := new(User)
	user1.Name = "bcd"

	users := make([]*User, 0)
	users = append(users, user)
	users = append(users, user1)
	df, _ := dataframe.NewFromStructs(users)

	dfLen := df.Len()
	fmt.Printf("%v\n", df.Len())

	for i := 0; i < dfLen; i++ {
		value, _ := df.At(i, "Name")
		unit := value.MustString()
		fmt.Printf("%v\n", unit)
	}
}