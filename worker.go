package main

import (
	localtypes "MRTest/types"
	"encoding/gob"
	"encoding/json"
	"log"
	"net/rpc"
	"os"
	"plugin"
	"runtime"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// Worker
type Worker struct {
	uid            string
	state          WorkerState
	currentTask    localtypes.Task
	mapFunction    func(string, string) []localtypes.Pair
	reduceFunction func(string, []string) string
	outputFolder   string
	reduceNum      int
	config         WorkerConfig
}

type WorkerState uint8

const (
	Init WorkerState = iota
	Idle
	Working
	Stopped
)

func (self WorkerState) String() string {
	switch self {
	case Init:
		return "Init"
	case Idle:
		return "Idle"
	case Working:
		return "Working"
	case Stopped:
		return "Stopped"
	default:
		return "Unknown WorkerState " + strconv.FormatInt(int64(self), 10)
	}
}

type WorkerConfigFile struct {
	Config WorkerConfig `json:"config"`
}

type WorkerConfig struct {
	Address string `json:"client.address"`
	Port    int    `json:"client.port"`
}

func NewWorker(pluginName string) *Worker {
	uid, err := localtypes.GenerateUID(6)
	if err != nil {
		log.Fatal("Generate error:", err)
	}
	var ref = &Worker{
		uid:          uid,
		state:        Init,
		currentTask:  localtypes.Task{},
		outputFolder: "",
	}

	// Only Linux can use go plugin; other platform use built-in func
	if runtime.GOOS == "linux" {
		ref.mapFunction, ref.reduceFunction = loadPlugin(pluginName)
		log.Println("[ - ] PluginLoader: Loaded functions from plugin")
	} else {
		ref.mapFunction = func(filename string, contents string) []localtypes.Pair {
			// function to detect word separators.
			ff := func(r rune) bool { return !unicode.IsLetter(r) }

			// split contents into an array of words.
			words := strings.FieldsFunc(contents, ff)

			kva := []localtypes.Pair{}
			for _, w := range words {
				kv := localtypes.Pair{w, "1"}
				kva = append(kva, kv)
			}
			return kva
		}
		ref.reduceFunction = func(key string, values []string) string {
			// return the number of occurrences of this word.
			return strconv.Itoa(len(values))
		}
		log.Println("[ - ] PluginLoader: Use built-in functions")
	}

	ref.loadConfig()
	return ref
}

func (self *Worker) Launch(client *rpc.Client) {
MainLoop:
	for {
		switch self.state {
		case Init:
			self.DoRegister(client)
			go self.KeepAlive(client)
		case Idle:
			self.DoSeekNewTask(client)
		case Working:
			self.DoPerformTask(client)
		case Stopped:
			self.DoUnregister(client)
			break MainLoop
		}
	}
}

func (self *Worker) DoRegister(client *rpc.Client) {
	log.Printf("[ ->] Self: Registering as \"%s\"...", self.uid)
	var req = localtypes.MessageRequest{
		WorkerID: self.uid,
		Type:     localtypes.ReqRegister,
	}
	var resp = localtypes.MessageResponse{}

	var err error
	err = client.Call("Coordinator.RegisterHandler", &req, &resp)
	if err != nil {
		log.Fatal("Register error:", err)
	}

	self.outputFolder = resp.Args[0].(string)
	self.reduceNum = resp.Args[1].(int)

	// 检查文件夹是否存在
	if _, err = os.Stat(self.outputFolder); os.IsNotExist(err) {
		// 文件夹不存在，创建文件夹
		err = os.MkdirAll(self.outputFolder, 0755)
		if err != nil {
			log.Fatalf("Error creating directory: %v", err)
		}
	} else {
		// 文件夹存在，检查是否为空
		var fis []os.DirEntry
		fis, err = os.ReadDir(self.outputFolder)
		if err != nil {
			log.Fatalf("Error reading directory: %v", err)
		}

		// 如果文件夹不为空，则报错退出
		if len(fis) > 0 {
			log.Fatal("Output folder not empty. Please clear it and retry.")
		}

		// 如果文件夹为空，什么也不做
	}

	self.state = Idle
	log.Println("[<- ] Server: Register success")
}

func (self *Worker) DoUnregister(client *rpc.Client) {
	log.Printf("[ ->] Self: Unregistering...")
	var req = localtypes.MessageRequest{
		WorkerID: self.uid,
		Type:     localtypes.ReqUnregister,
	}
	resp := localtypes.MessageResponse{}

	err := client.Call("Coordinator.UnregisterHandler", &req, &resp)
	if err != nil {
		log.Fatal("Unregister error:", err)
	}

	self.state = Stopped
	log.Println("[<- ] Server: Unregister success")
}

func (self *Worker) DoSeekNewTask(client *rpc.Client) {
	log.Printf("[ ->] Self: Request for a new task")
	var req = localtypes.MessageRequest{
		WorkerID: self.uid,
		Type:     localtypes.ReqSeekTask,
	}
	var resp = localtypes.MessageResponse{}

	// Seek for a new task
	err := client.Call("Coordinator.TaskAllocator", &req, &resp)
	if err != nil {
		log.Fatal("TaskAllocate error:", err)
	}

	// Parse the response
	switch resp.Type {
	case localtypes.RespWait:
		self.state = Idle
		//log.Printf("[<- ] Server: Asked to wait")
		time.Sleep(200 * time.Millisecond)
	case localtypes.RespNewTask:
		self.state = Working
		self.currentTask = resp.Args[0].(localtypes.Task)
		log.Printf("[<- ] Server: %s Task \"%s\" assigned", self.currentTask.Type, self.currentTask.TaskID)
	case localtypes.RespHalt:
		self.state = Stopped
		log.Printf("[<- ] Server: Asked to stop")
	default:
		log.Fatal("Unknown Type:", resp.Type)
	}
}

func (self *Worker) DoPerformTask(client *rpc.Client) {
	var err error
	log.Printf("[ - ] Task \"%s\": Performing", self.currentTask.TaskID)

	var req = localtypes.MessageRequest{
		WorkerID: self.uid,
	}
	var reqTask = self.currentTask // 值传递

	switch self.currentTask.Type {
	case localtypes.Map:
		mappedResult := self.mapFunction(self.currentTask.Args[0].(string), self.currentTask.Args[1].(string))

		// 按ReduceNo分类存放
		// type(intermediate) = [reduceNo]PairSlice = [reduceNo][]Pair
		intermediate := make([]localtypes.PairSlice, self.reduceNum)
		for _, pair := range mappedResult {
			var hash int
			hash, err = localtypes.GetHash(pair.Key)
			if err != nil {
				log.Fatal("GetHash error:", err)
			}
			intermediate[hash%self.reduceNum] = append(intermediate[hash%self.reduceNum], pair)
		}
		reqTask.Args = []interface{}{intermediate} // 返回结果给Coordinator

	case localtypes.Reduce:
		intermediate := make(map[string][]string)
		for _, pair := range self.currentTask.Args[0].(localtypes.PairSlice) {
			intermediate[pair.Key] = append(intermediate[pair.Key], pair.Value)
		}
		result := localtypes.PairSlice{}
		for key, value := range intermediate {
			result = append(result, localtypes.Pair{
				Key:   key,
				Value: self.reduceFunction(key, value),
			})
		}

		filename := self.outputFolder + "/mr-out-" + self.currentTask.TaskID
		var file *os.File
		file, err = os.Create(filename)
		if err != nil {
			log.Fatal("Create file failed:", err)
		}
		defer func(tmpFile *os.File) {
			err := tmpFile.Close()
			if err != nil {
				log.Fatal("Close file failed:", err)
			}
		}(file)

		for _, pair := range result {
			_, err = file.Write([]byte(pair.Key + " " + pair.Value + "\n"))
			if err != nil {
				log.Fatal("Error writing to file:", err)
			}
		}

		reqTask.Args = []interface{}{}
	}

	reqTask.State = localtypes.TaskDone
	req.Args = []interface{}{reqTask}
	req.Type = localtypes.ReqTaskDone
	var resp = localtypes.MessageResponse{}
	log.Printf("[ - ] Task \"%s\": Done", self.currentTask.TaskID)

	// Notify the server current job done
	err = client.Call("Coordinator.DoneWorkHandler", &req, &resp)
	if err != nil {
		log.Fatal("DoneWork error:", err)
	}

	self.currentTask = localtypes.Task{}
	self.state = Idle
	return
}

func (self *Worker) KeepAlive(client *rpc.Client) {
	var req = localtypes.MessageRequest{
		WorkerID: self.uid,
		Type:     localtypes.ReqKeepAlive,
	}
	var resp = localtypes.MessageResponse{}

	for self.state != Stopped {
		err := client.Call("Coordinator.HeartbeatHandler", &req, &resp)
		if err != nil {
			log.Fatal("Heartbeat error:", err)
		}

		resp = localtypes.MessageResponse{}
		time.Sleep(1 * time.Second)
	}
}

func (self *Worker) loadConfig() {
	var cfg = WorkerConfigFile{
		Config: WorkerConfig{
			Address: "localhost",
			Port:    1234,
		},
	}

	fileContent, err := os.ReadFile("worker.json")
	if err != nil {
		if os.IsNotExist(err) {
			log.Println("[ - ] ConfigLoader: File not found, using default value")
			var err error
			fileNew, err := os.Create("worker.json")
			jsonData, err := json.Marshal(cfg)
			if err != nil {
				log.Fatal("Error encoding json:", err)
			}
			_, err = fileNew.Write(jsonData)
			if err != nil {
				log.Fatal("Error writing file:", err)
			}
		} else {
			log.Fatal("Error reading file:", err)
		}
	} else {
		log.Println("[ - ] ConfigLoader: Config loaded")
		err = json.Unmarshal(fileContent, &cfg)
		if err != nil {
			log.Fatal("Error decoding JSON:", err)
		}
	}

	self.config = cfg.Config
}

func loadPlugin(filename string) (func(string, string) []localtypes.Pair, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []localtypes.Pair)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Usage: worker [pluginName.so]\n")
	}

	worker := NewWorker(os.Args[1])
	gob.Register(localtypes.Task{})
	gob.Register(localtypes.PairSlice{})
	gob.Register([]localtypes.PairSlice{})

	// Establish and defer Close connection
	client, err := rpc.Dial("tcp", worker.config.Address+":"+strconv.Itoa(worker.config.Port))
	if err != nil {
		log.Fatal("Dial error:", err)
	}
	defer func(client *rpc.Client) {
		err := client.Close()
		if err != nil {
			log.Fatal("Close error:", err)
		}
	}(client)

	// Launch
	worker.Launch(client)
}
