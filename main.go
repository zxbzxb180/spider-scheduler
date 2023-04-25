package main

import (
	"encoding/json"
	"log"
	"time"

	"github.com/go-redis/redis"
	"github.com/jasonlvhit/gocron"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
)

const (
	RedisURLKey      = "urls"
	RedisSeedKey     = "seeds"
	RedisVisitedKey  = "visited"
	RedisTasksKey    = "tasks"
	RedisWorkerQueue = "worker"
)

// URL 结构体
type URL struct {
	gorm.Model
	Url string `gorm:"not null;unique_index"`
}

// Seed 结构体
type Seed struct {
	gorm.Model
	Url string `gorm:"not null;unique_index"`
}

// Task 爬虫任务
type Task struct {
	gorm.Model
	Name   string
	Url    string
	Status TaskStatus `gorm:"default:'stopped'"`
}

// 爬虫任务状态枚举
type TaskStatus string

const (
	Stopped TaskStatus = "stopped"
	Running TaskStatus = "running"
	Paused  TaskStatus = "paused"
)

// 定义爬虫函数类型
type SpiderFunc func(url string)

// 数据库连接
var db *gorm.DB

// Redis客户端
var rdb *redis.Client

// 调度器结构体
type Scheduler struct {
}

// 创建调度器实例
func NewScheduler() *Scheduler {
	return &Scheduler{}
}

// 定义爬虫函数
func spider(url string) {
	log.Printf("正在爬取：%s\n", url)
	// 爬取页面并处理数据...
	time.Sleep(time.Second)
	// 记录已经爬取的URL
	addVisitedURL(url)
	// 获取新的URL并添加到任务队列
	urls, err := getNewURLs(url)
	if err != nil {
		log.Println(err)
	}
	for _, u := range urls {
		addURL(u)
	}
}

// 启动指定的爬虫任务
func StartTask(task *Task) error {
	task.Status = Running
	return db.Save(task).Error
}

// 暂停指定的爬虫任务
func PauseTask(task *Task) error {
	task.Status = Paused
	return db.Save(task).Error
}

// 停止指定的爬虫任务
func StopTask(task *Task) error {
	task.Status = Stopped
	return db.Save(task).Error
}

// 添加URL到Redis去重集合中
func addURL(url string) {
	rdb.SAdd(RedisURLKey, url)
}

// 添加已经爬取的URL到Redis有序集合中
func addVisitedURL(url string) {
	rdb.ZAdd(RedisVisitedKey, redis.Z{Score: float64(time.Now().Unix()), Member: url})
}

// 获取新的URL列表
func getNewURLs(url string) ([]string, error) {
	// 爬取页面并解析出新的URL...
	return []string{"http://www.example.com/new1", "http://www.example.com/new2", "http://www.example.com/new3"}, nil
}

// 添加爬虫任务到Redis任务队列中
func AddTaskToQueue(task *Task) error {
	taskJSON, err := json.Marshal(task)
	if err != nil {
		return err
	}
	rdb.LPush(RedisTasksKey, string(taskJSON))
	return nil
}

// 定时调度爬虫任务
func ScheduleTask(name, url string, priority int32, interval uint64) {
	// 添加Seed到MySQL中
	db.Create(&Seed{Url: url})
	// 添加爬虫任务到MySQL中
	task := &Task{Name: name, Url: url, Status: Stopped}
	db.Create(task)
	// 定时调度任务
	gocron.Every(interval).Seconds().Do(func() {
		// 从Redis任务队列中获取任务
		taskJSON, err := rdb.RPop(RedisTasksKey).Result()
		if err != nil {
			log.Println(err)
			return
		}
		var t Task
		err = json.Unmarshal([]byte(taskJSON), &t)
		if err != nil {
			log.Println(err)
			return
		}
		// 将任务添加到Worker队列中
		rdb.LPush(RedisWorkerQueue, t.ID)
	})
}

func main() {
	// 连接MySQL数据库
	var err error
	db, err = gorm.Open("mysql", "user:password@tcp(ip:port)/database_name?charset=utf8&parseTime=True&loc=Asia%2FShanghai")
	if err != nil {
		panic(err)
	}
	defer db.Close()
	db.AutoMigrate(&URL{}, &Seed{}, &Task{}) // 创建URL、Seed和Task数据表

	// 连接Redis
	rdb = redis.NewClient(&redis.Options{
		Addr:     "ip:port",
		Password: "",
		DB:       0,
	})
	_, err = rdb.Ping().Result()
	if err != nil {
		panic(err)
	}

	scheduler := NewScheduler()

	// 定时调度爬虫任务
	ScheduleTask("Job1", "http://www.example.com", 1, 2)

	// 执行Worker程序
	for {
		taskID, err := rdb.RPopLPush(RedisWorkerQueue, RedisWorkerQueue).Result()
		if err != nil {
			log.Println(err)
			time.Sleep(time.Second)
			continue
		}
		var task Task
		db.First(&task, taskID)
		if task.Status != Running {
			continue
		}
		spider(task.Url)
	}
}
