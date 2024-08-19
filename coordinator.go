package main

import (
	localtypes "MRTest/types"
	"bufio"
	"encoding/gob"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
)

// Coordinator
type Coordinator struct {
	state           CoordinatorState
	lock            sync.Mutex
	workerList      map[string]chan struct{}
	mapTaskList     map[string]localtypes.Task // TaskID as key
	reduceTaskList  map[string]localtypes.Task // TaskID as key
	taskQueue       chan string                // TaskID as key
	outputFolder    string
	mapResultBuffer []localtypes.PairSlice // [reduceNo]PairSlice == [reduceNo][]Pair
	config          CoordinatorConfig
}

type CoordinatorState uint8

const (
	InitMap CoordinatorState = iota
	WaitingWorker
	WaitingMap
	InitReduce
	WaitingReduce
	Done
)

func (self CoordinatorState) String() string {
	switch self {
	case InitMap:
		return "Busy(Map)"
	case InitReduce:
		return "Busy(Reduce)"
	case WaitingMap:
		return "Waiting(Map)"
	case WaitingReduce:
		return "Waiting(Reduce)"
	case WaitingWorker:
		return "Busy(Worker)"
	case Done:
		return "Done"
	default:
		return "Unknown CoordinatorState " + strconv.FormatInt(int64(self), 10)
	}
}

type CoordinatorConfigFile struct {
	Config CoordinatorConfig `json:"config"`
}

type CoordinatorConfig struct {
	Port      int    `json:"server.port"`
	RegMode   string `json:"server.reg.mode"`
	RegArg    int    `json:"server.reg.arg"`
	ChunkSize string `json:"mr.map.chunkSize"`
	ReduceNum int    `json:"mr.reduce.num"`
}

func NewCoordinator(inputDir string, outputDir string) *Coordinator {
	var ref = &Coordinator{
		// state:           Will init later
		// lock:            No need to init
		workerList:     make(map[string]chan struct{}),
		mapTaskList:    make(map[string]localtypes.Task),
		reduceTaskList: make(map[string]localtypes.Task),
		// taskQueue:       Will Init in ref.mapTaskPlanner()
		outputFolder: outputDir,
		// mapResultBuffer: Will init later
	}

	ref.loadConfig()
	ref.mapResultBuffer = make([]localtypes.PairSlice, ref.config.ReduceNum)

	ref.state = InitMap
	ref.mapTaskPlanner(inputDir)

	return ref
}

// mapTaskPlanner 不带锁
func (self *Coordinator) mapTaskPlanner(dir string) {
	log.Println("[ - ] MapTaskPlanner: Loading inputs from \"" + dir + "\" (might take a while)...")

	var fileCounter = 0
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}

		// Skip hidden files and directories
		if d.Name()[0] == '.' {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}

		if !d.IsDir() { // Ensure it's a file
			log.Println("[ - ] MapTaskPlanner: Found file", d.Name())
			self.processFile(path)
			fileCounter++
		}

		return nil
	})
	if err != nil {
		log.Fatal("Error walking the directory:", err)
	}

	// Push tasks into channel
	self.taskQueue = make(chan string, func() int {
		mapTasksNum := len(self.mapTaskList)
		if mapTasksNum > self.config.ReduceNum {
			return mapTasksNum
		} else {
			return self.config.ReduceNum
		}
	}())
	for _, task := range self.mapTaskList {
		self.taskQueue <- task.TaskID
	}

	self.state = WaitingWorker
	log.Printf("[ - ] MapTaskPlanner: %d tasks were found in %d files (Queue %d/%d)",
		len(self.mapTaskList), fileCounter, len(self.taskQueue), cap(self.taskQueue))
}

// processFile
// Call Stack: mapTaskPlanner->processFile
func (self *Coordinator) processFile(path string) {
	file, err := os.Open(path)
	if err != nil {
		log.Fatalf("Error opening file %s: %v", path, err)
		return
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Fatalf("Error closing file %s: %v", path, err)
		}
	}(file)

	reader := bufio.NewReader(file)
	var chunk strings.Builder
	chunkSize, err := localtypes.ParseFileSize(self.config.ChunkSize)
	if err != nil {
		log.Fatal("Error parsing file size:", err)
	}
	currentSize := 0

	for {
		line, err := reader.ReadString(' ')
		if err != nil && err != io.EOF {
			log.Printf("Error reading file %s: %v", path, err)
			return
		}

		// Remove punctuation marks from the line
		var result strings.Builder
		for _, r := range line {
			if unicode.IsPunct(r) {
				result.WriteRune(' ')
			} else {
				result.WriteRune(r)
			}
		}
		filteredLine := result.String()

		// Add chunks
		if currentSize+len(filteredLine) > chunkSize && chunk.Len() > 0 {
			self.addChunk(path, chunk.String())
			chunk.Reset()
			currentSize = 0
		}

		chunk.WriteString(filteredLine)
		currentSize += len(filteredLine)

		if err == io.EOF {
			if chunk.Len() > 0 {
				self.addChunk(path, chunk.String())
			}
			break
		}
	}
}

// addChunk
// Call Stack: mapTaskPlanner->processFile->addChunk
func (self *Coordinator) addChunk(path, content string) {
	uid, err := localtypes.GenerateUID(8)
	if err != nil {
		log.Fatal("Error generating task id:", err)
	}

	self.mapTaskList[uid] = localtypes.Task{
		TaskID:   uid,
		WorkerID: "",
		Type:     localtypes.Map,
		State:    localtypes.TaskPending,
		Args:     []interface{}{path, content},
	}
}

// mapTaskPlanner 不带锁
func (self *Coordinator) reduceTaskPlanner() {
	log.Printf("[ - ] ReduceTaskPlanner: Generating %d Reduce task(s) (might take a while)...", self.config.ReduceNum)
	for reduceNo := 0; reduceNo < self.config.ReduceNum; reduceNo++ {
		uid, err := localtypes.GenerateUID(8)
		if err != nil {
			log.Fatal("Error generating task id:", err)
		}

		self.reduceTaskList[uid] = localtypes.Task{
			TaskID:   uid,
			WorkerID: "",
			Type:     localtypes.Reduce,
			State:    localtypes.TaskPending,
			Args:     []interface{}{self.mapResultBuffer[reduceNo]},
		}
	}

	// Push tasks into channel
	for _, task := range self.reduceTaskList {
		self.taskQueue <- task.TaskID
	}

	self.state = WaitingReduce
	log.Printf("[ - ] ReduceTaskPlanner: %d tasks created (Queue %d/%d)",
		len(self.reduceTaskList), len(self.taskQueue), cap(self.taskQueue))
}

/****** Handlers Begin ******/

// RegisterHandler Mutex
func (self *Coordinator) RegisterHandler(req *localtypes.MessageRequest, resp *localtypes.MessageResponse) error {
	self.lock.Lock()
	defer self.lock.Unlock()

	log.Printf("[<- ] Worker \"%s\": Request for Registration", req.WorkerID)

	if req.Type != localtypes.ReqRegister {
		return errors.New("invalid message type")
	}
	if _, exists := self.workerList[req.WorkerID]; exists {
		return errors.New("client already registered")
	}

	heartbeatChan := make(chan struct{}, 1)
	self.workerList[req.WorkerID] = heartbeatChan

	go self.monitorClient(req.WorkerID, heartbeatChan)

	if self.state != Done {
		switch self.config.RegMode {
		case "worker":
			if len(self.workerList) >= self.config.RegArg {
				self.state = WaitingMap
			} else {
				self.state = WaitingWorker
			}
		case "time":
			break
		default:
			self.state = WaitingMap
		}
	}

	resp.Type = localtypes.RespReceived
	resp.Args = append(resp.Args, self.outputFolder)
	resp.Args = append(resp.Args, self.config.ReduceNum)
	return nil
}

// UnregisterHandler Mutex
func (self *Coordinator) UnregisterHandler(req *localtypes.MessageRequest, resp *localtypes.MessageResponse) error {
	// 本质是处理注销信号，并向monitorClient方法发送关闭信号。
	self.lock.Lock()
	defer self.lock.Unlock()

	log.Printf("[<- ] Worker \"%s\": Request for Unregistration", req.WorkerID)

	heartbeatChan, exists := self.workerList[req.WorkerID]
	if req.Type != localtypes.ReqUnregister {
		return errors.New("invalid message type")
	}
	if !exists {
		return errors.New("client not found (is the client already unregistered?)")
	}

	// Signal the monitoring goroutine to stop
	// 除关闭channel以外的所有关闭操作均在monitorClient方法完成
	close(heartbeatChan)

	resp.Type = localtypes.RespReceived
	return nil
}

// HeartbeatHandler Mutex
func (self *Coordinator) HeartbeatHandler(req *localtypes.MessageRequest, resp *localtypes.MessageResponse) error {
	self.lock.Lock()
	defer self.lock.Unlock()

	heartbeatChan, exists := self.workerList[req.WorkerID]

	if req.Type != localtypes.ReqKeepAlive {
		return errors.New("invalid message type")
	}
	if !exists {
		return errors.New("client not registered")
	}

	// Non-blocking send to signal heartbeat
	select {
	case heartbeatChan <- struct{}{}:
	default:
	}

	resp.Type = localtypes.RespReceived
	return nil
}

// TaskAllocator Mutex
func (self *Coordinator) TaskAllocator(req *localtypes.MessageRequest, resp *localtypes.MessageResponse) error {
	self.lock.Lock()
	defer self.lock.Unlock()

	if req.Type != localtypes.ReqSeekTask {
		return errors.New("invalid message type")
	}
	if _, exists := self.workerList[req.WorkerID]; !exists {
		return errors.New("client not registered")
	}

	log.Printf("[<- ] Worker \"%s\": Request for a new task", req.WorkerID)
	switch self.state {
	case InitMap:
		fallthrough
	case InitReduce:
		log.Printf("[ ->] Worker \"%s\": Ask to wait (No task at the moment)", req.WorkerID)
		resp.Type = localtypes.RespWait
	case WaitingWorker:
		log.Printf("[ ->] Worker \"%s\": Ask to wait (Other worker not ready)", req.WorkerID)
		resp.Type = localtypes.RespWait
	case WaitingMap:
		select {
		case taskID := <-self.taskQueue:
			var task = self.mapTaskList[taskID]
			task.State = localtypes.TaskAssigned
			task.WorkerID = req.WorkerID
			self.mapTaskList[taskID] = task
			resp.Type = localtypes.RespNewTask
			resp.Args = append(resp.Args, task)
			log.Printf("[ ->] Worker \"%s\": Assigned for map task \"%s\" (Queue %d/%d)",
				req.WorkerID, taskID, len(self.taskQueue), cap(self.taskQueue))
		default: // Flow control
			log.Printf("[ ->] Worker \"%s\": Ask to wait (No task at the moment)", req.WorkerID)
			resp.Type = localtypes.RespWait
		}
	case WaitingReduce:
		select {
		case taskID := <-self.taskQueue:
			var task = self.reduceTaskList[taskID]
			task.State = localtypes.TaskAssigned
			task.WorkerID = req.WorkerID
			self.reduceTaskList[taskID] = task
			resp.Type = localtypes.RespNewTask
			resp.Args = append(resp.Args, task)
			log.Printf("[ ->] Worker \"%s\": Assigned for reduce task \"%s\" (Queue %d/%d)",
				req.WorkerID, taskID, len(self.taskQueue), cap(self.taskQueue))
		default: // Flow control
			log.Printf("[ ->] Worker \"%s\": Ask to wait (No task at the moment)", req.WorkerID)
			resp.Type = localtypes.RespWait
		}
	case Done:
		log.Printf("[ ->] Worker \"%s\": Ask to stop (Tasks all done)", req.WorkerID)
		resp.Type = localtypes.RespHalt
	}

	return nil
}

// DoneWorkHandler Mutex
func (self *Coordinator) DoneWorkHandler(req *localtypes.MessageRequest, resp *localtypes.MessageResponse) error {
	self.lock.Lock()
	defer self.lock.Unlock()

	if req.Type != localtypes.ReqTaskDone {
		return errors.New("invalid message type")
	}
	if _, exists := self.workerList[req.WorkerID]; !exists {
		return errors.New("client not registered")
	}

	var task = req.Args[0].(localtypes.Task)
	if task.Type == localtypes.Map {
		for reduceNo := 0; reduceNo < self.config.ReduceNum; reduceNo++ {
			self.mapResultBuffer[reduceNo] = append(self.mapResultBuffer[reduceNo], task.Args[0].([]localtypes.PairSlice)[reduceNo]...)
		}
	}

	switch task.State {
	case localtypes.TaskDone:
		log.Printf("[<- ] Worker \"%s\": Task \"%s\" completed", req.WorkerID, task.TaskID)
	default:
		log.Printf("[<- ] Worker \"%s\": Task \"%s\" failed", req.WorkerID, task.TaskID)
		self.reQueueTaskByTaskID(task.TaskID)
	}

	switch task.Type {
	case localtypes.Map:
		self.mapTaskList[req.Args[0].(localtypes.Task).TaskID] = task
	case localtypes.Reduce:
		self.reduceTaskList[req.Args[0].(localtypes.Task).TaskID] = task
	}
	resp.Type = localtypes.RespReceived

	// State check
	if len(self.taskQueue) == 0 {
		if self.state == WaitingMap && self.isMapTasksDone() {
			log.Println("[ - ] StateChecker: All Map Tasks Done")
			self.state = InitReduce
			self.reduceTaskPlanner()
		}
		if self.state == WaitingReduce && self.isReduceTasksDone() {
			log.Println("[ - ] StateChecker: All Reduce Tasks Done")
			self.state = Done
		}
		if self.Done() {
			log.Println("[ - ] StateChecker: All Tasks Done")
		}
	}

	return nil
}

/****** Handlers End ******/

func (self *Coordinator) monitorClient(uid string, heartbeatChan chan struct{}) {
	timeout := 5 * time.Second
	timer := time.NewTimer(timeout)

	for {
		select {
		case _, ok := <-heartbeatChan:
			// 两种情况：收到心跳包或收到注销信号
			// 对于收到心跳包，需要关闭原计时器开启新计时器；
			// 对于收到注销信号，需要关闭计时器防止计时器触发关闭两次channel (panic)
			// 因此无论如何都需要关闭计时器
			if !timer.Stop() {
				<-timer.C // Drain the channel
			}

			// 收到注销信号（本质是收到channel关闭信号）
			// 没有锁是因为触发该条件时外层一定有锁
			if !ok {
				delete(self.workerList, uid)
				self.reQueueTasksByWorkerID(uid)
				log.Println("[ - ] ClientTracker: Stop tracking worker \"" + uid + "\" (Unregistered)")
				return
			}
			// 否则代表收到了心跳包，继续监听
			timer.Reset(timeout)

		case <-timer.C:
			// Timeout
			self.lock.Lock()
			delete(self.workerList, uid)
			close(heartbeatChan)
			self.reQueueTasksByWorkerID(uid)
			self.lock.Unlock()
			log.Println("[ - ] Self: Stop tracking worker \"" + uid + "\" (Timeout)")
			return
		}
	}
}

// isMapTasksDone 不带锁
func (self *Coordinator) isMapTasksDone() bool {
	for _, task := range self.mapTaskList {
		if task.State != localtypes.TaskDone {
			return false
		}
	}
	return true
}

// isMapTasksDone 不带锁
func (self *Coordinator) isReduceTasksDone() bool {
	for _, task := range self.reduceTaskList {
		if task.State != localtypes.TaskDone {
			return false
		}
	}
	return true
}

// reQueueTasksByWorkerID 不带锁
func (self *Coordinator) reQueueTasksByWorkerID(uid string) {
	counter := 0

	var taskList map[string]localtypes.Task
	switch self.state {
	case WaitingMap:
		taskList = self.mapTaskList
	case WaitingReduce:
		taskList = self.reduceTaskList
	case Done:
		return
	default:
		log.Fatal("[ - ] RequeueTask: Invalid state")
	}

	for _, task := range taskList {
		if task.WorkerID == uid && task.State != localtypes.TaskDone {
			counter++
			task.WorkerID = ""
			task.State = localtypes.TaskPending
			taskList[task.TaskID] = task
			self.taskQueue <- task.TaskID
		}
	}
	if counter != 0 {
		log.Printf("[ - ] Self: Worker \"%s\" abandoned %d task(s), marking them all as retry-required", uid, counter)
	}
}

// reQueueTaskByTaskID 不带锁
func (self *Coordinator) reQueueTaskByTaskID(uid string) {
	var task localtypes.Task
	switch self.state {
	case WaitingMap:
		task = self.mapTaskList[uid]
	case WaitingReduce:
		task = self.reduceTaskList[uid]
	default:
		log.Fatal("[ - ] RequeueTask: Invalid state")
	}

	task.WorkerID = ""
	task.State = localtypes.TaskPending
	self.taskQueue <- task.TaskID

	log.Println("[ - ] Self: Task \"" + uid + "\" marked as retry-required")
}

func (self *Coordinator) loadConfig() {
	var cfg = CoordinatorConfigFile{
		Config: CoordinatorConfig{
			Port:      1234,
			RegMode:   "worker",
			RegArg:    2,
			ChunkSize: "256KB",
			ReduceNum: 3,
		},
	}

	fileContent, err := os.ReadFile("coordinator.json")
	if err != nil {
		if os.IsNotExist(err) {
			log.Println("[ - ] ConfigLoader: File not found, using default value")
			var err error
			fileNew, err := os.Create("coordinator.json")
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

/****** Adapters Begin ******/

func (self *Coordinator) Done() bool {
	return self.state == Done
}

/****** Adapters End ******/

func main() {
	if len(os.Args) < 3 {
		log.Fatal("Usage: coordinator [inputFolder] [outputFolder]\n")
	}

	var err error
	coordinator := NewCoordinator(os.Args[1], os.Args[2])
	defer close(coordinator.taskQueue)
	gob.Register(localtypes.Task{})
	gob.Register(localtypes.PairSlice{})
	gob.Register([]localtypes.PairSlice{})

	// Start RPC Service
	err = rpc.Register(coordinator)
	if err != nil {
		log.Fatal("Register error:", err)
	}
	listener, err := net.Listen("tcp", ":"+strconv.Itoa(coordinator.config.Port))
	if err != nil {
		log.Fatal("Listen error:", err)
	}
	defer func(listener net.Listener) {
		err := listener.Close()
		if err != nil {
			log.Fatal("Close error:", err)
		}
	}(listener)
	log.Printf("[ - ] Self: Server is listening on port %d", coordinator.config.Port)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("[ - ] Self: Accept error:", err)
			continue
		}
		go rpc.ServeConn(conn)
	}
}
