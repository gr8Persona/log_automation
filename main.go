package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"
	"time"
)

type Time struct {
	Start    string `json:"start"`
	end      string
	Duration string `json:"duration"`
}

type Address struct {
	From string `json:"from"`
	To   string `json:"to"`
}

type Session struct {
	Time      Time    `json:"time"`
	SessionID string  `json:"sessionid"`
	Client    string  `json:"client"`
	MessageID string  `json:"messageid"`
	Address   Address `json:"address"`
	Status    string  `json:"status"`
}

type Logs struct {
	Sessions map[string]*Session // SessionID - LOG
}

func main() {
	inFilePtr := flag.String("log", "", "file path to log file")
	flag.Parse()

	if fs, e := os.Stat(*inFilePtr); os.IsNotExist(e) {
		showErrorAndExit(fmt.Errorf("specified log file does not exit"))
	} else if fs.IsDir() {
		showErrorAndExit(fmt.Errorf("specified log path is not a file"))
	}

	logs := Logs{
		Sessions: make(map[string]*Session),
	}

	// Open logFile
	file, e := os.Open(*inFilePtr)
	if e != nil {
		showErrorAndExit(fmt.Errorf("can't open logs file - %s", e))
	}
	defer file.Close()

	// read and parse line by line
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		if e := logs.parseLine(scanner.Text()); e != nil {
			showErrorAndExit(e)
		}
	}

	if e := scanner.Err(); e != nil {
		showErrorAndExit(e)
	}
	// get only full sessions
	fullSessions := logs.getFullSessions()
	jsonData, e := jsonOut(fullSessions)
	if e != nil {
		showErrorAndExit(e)
	}
	fmt.Printf("%s\n", jsonData)
}

func (l *Logs) getFullSessions() []*Session {
	sess := make([]*Session, 0)
	for _, s := range l.Sessions {
		if s.isSessionFull() {
			sess = append(sess, s)
		}
	}
	return sess
}

func (l Logs) parseLine(line string) error {
	// clean up multiple spaces
	rSpaces := regexp.MustCompile(`(\s)+`)
	line = rSpaces.ReplaceAllString(line, " ")

	outList := strings.Split(line, " ")
	if len(outList) != 3 {
		return fmt.Errorf("more than 3 objects in line '%s'", line)
	}
	sessionID := outList[1]
	// Add session if new
	if _, exist := l.Sessions[sessionID]; !exist {
		newLog := Session{
			SessionID: sessionID,
		}
		l.Sessions[sessionID] = &newLog
	}
	logDataList := strings.Split(outList[2], "=")
	if len(logDataList) != 2 {
		return fmt.Errorf("should be key and value in '%s'", outList[2])
	}
	key := logDataList[0]
	// handle data
	if key == "client" {
		l.Sessions[sessionID].Time.Start = outList[0]
		l.Sessions[sessionID].Client = logDataList[1]
	} else if key == "message-id" {
		l.Sessions[sessionID].MessageID = logDataList[1]
	} else if key == "from" {
		l.Sessions[sessionID].Address.From = logDataList[1]
	} else if key == "to" {
		l.Sessions[sessionID].Address.To = logDataList[1]
	} else if key == "status" {
		l.Sessions[sessionID].Status = logDataList[1]
		l.Sessions[sessionID].Time.end = outList[0]
		l.Sessions[sessionID].Time.SetDuration()
	}
	return nil
}

func jsonOut(d interface{}) (string, error) {
	buf := new(bytes.Buffer)
	enc := json.NewEncoder(buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "	")

	if e := enc.Encode(&d); e != nil {
		return "", e
	}
	return buf.String(), nil
}

func (s *Session) isSessionFull() bool {
	if s.Time.Start == "" || s.Time.Duration == "" || s.Client == "" || s.MessageID == "" || s.SessionID == "" || s.Status == "" || s.Address.From == "" || s.Address.To == "" {
		return false
	} else {
		return true
	}
}

func (t *Time) SetDuration() error {
	if t.end == "" || t.Start == "" {
		return nil
	}
	layout := "2006-01-02T15:04:05.999999"
	durationLayout := "15:04:05.999999"
	tEnd, e := time.Parse(layout, t.Start)
	if e != nil {
		return e
	}
	tStart, e := time.Parse(layout, t.end)
	if e != nil {
		return e
	}
	diff := tStart.Sub(tEnd)
	outDiff := time.Time{}.Add(diff)
	t.Duration = outDiff.Format(durationLayout)

	return nil
}

func showErrorAndExit(e error) {
	log.Printf("[ERROR]: %s\n", e)
	os.Exit(0)
}
