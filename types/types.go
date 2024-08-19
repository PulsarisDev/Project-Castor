package types

import (
	"crypto/rand"
	"errors"
	"hash/fnv"
	"math/big"
	"regexp"
	"strconv"
	"strings"
)

/******* types *******/

// Pair 键值对
type Pair struct {
	Key   string
	Value string
}

type PairSlice []Pair

func (a PairSlice) Len() int           { return len(a) }
func (a PairSlice) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a PairSlice) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Task 任务类型
type Task struct {
	TaskID   string
	WorkerID string
	Type     TaskType
	State    TaskState
	Args     []interface{}
}

type TaskType uint8

const (
	Map TaskType = iota
	Reduce
)

func (self TaskType) String() string {
	switch self {
	case Map:
		return "Map"
	case Reduce:
		return "Reduce"
	default:
		return "Unknown TaskType " + strconv.FormatInt(int64(self), 10)
	}
}

type TaskState uint8

const (
	TaskPending TaskState = iota
	TaskAssigned
	TaskDone
)

func (self TaskState) String() string {
	switch self {
	case TaskPending:
		return "Pending"
	case TaskAssigned:
		return "Assigned"
	case TaskDone:
		return "Done"
	default:
		return "Unknown TaskState " + strconv.FormatInt(int64(self), 10)
	}
}

// MessageRequest
type MessageRequest struct {
	WorkerID string
	Type     MessageRequestType
	Args     []interface{}
}

type MessageRequestType uint8

const (
	ReqKeepAlive MessageRequestType = iota
	ReqRegister
	ReqUnregister
	ReqSeekTask
	ReqTaskDone
)

func (self MessageRequestType) String() string {
	switch self {
	case ReqKeepAlive:
		return "(Request)KeepAlive"
	case ReqRegister:
		return "(Request)Register"
	case ReqUnregister:
		return "(Request)Unregister"
	case ReqSeekTask:
		return "(Request)SeekForTask"
	case ReqTaskDone:
		return "(Request)TaskDone"
	default:
		return "Unknown RequestType " + strconv.FormatInt(int64(self), 10)
	}
}

// MessageResponse
type MessageResponse struct {
	Type MessageResponseType
	Args []interface{}
}

type MessageResponseType uint8

const (
	RespReceived MessageResponseType = iota
	RespWait
	RespNewTask
	RespHalt
)

func (self MessageResponseType) String() string {
	switch self {
	case RespReceived:
		return "(Response)ReqReceived"
	case RespWait:
		return "(Response)Wait"
	case RespNewTask:
		return "(Response)NewTask"
	case RespHalt:
		return "(Response)Halt"
	default:
		return "Unknown ResponseType " + strconv.FormatInt(int64(self), 10)
	}
}

/******* functions *******/

// GenerateUID 生成指定长度的UID
func GenerateUID(length int) (string, error) {
	const charset = "0123456789abcdef"
	uid := make([]byte, length)
	for i := range uid {
		num, err := rand.Int(rand.Reader, big.NewInt(int64(len(charset))))
		if err != nil {
			return "", err
		}
		uid[i] = charset[num.Int64()]
	}
	return string(uid), nil
}

func GetHash(str string) (int, error) {
	h := fnv.New32a()
	_, err := h.Write([]byte(str))
	if err != nil {
		return -1, err
	}
	return int(h.Sum32() & 0x7fffffff), nil
}

func ParseFileSize(sizeStr string) (int, error) {
	re := regexp.MustCompile(`^(\d+)([kKmMgG]?[bB]?)?$`)

	matches := re.FindStringSubmatch(sizeStr)
	if len(matches) != 3 {
		return 0, errors.New("invalid input format")
	}

	// 数字部分
	number, err := strconv.ParseFloat(matches[1], 64)
	if err != nil {
		return 0, err
	}

	// 单位部分
	unit := strings.ToUpper(strings.TrimSpace(matches[2]))

	// 倍率
	multiplier := 1.0
	switch unit {
	case "":
		fallthrough
	case "B":
		multiplier *= 1.0
	case "K":
		fallthrough
	case "KB":
		multiplier *= 1024
	case "M":
		fallthrough
	case "MB":
		multiplier *= 1024 * 1024
	case "G":
		fallthrough
	case "GB":
		if number > 1.0 {
			return 0, errors.New("size too big")
		}
		multiplier *= 1024 * 1024 * 1024
	default:
		return 0, errors.New("unsupported unit")
	}

	// 转换为字节
	return int(number * multiplier), nil
}
