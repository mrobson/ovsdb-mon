package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"reflect"
	"strings"
	"sync"
	"time"
	"github.com/kylelemons/godebug/diff"
	"github.com/kylelemons/godebug/pretty"
	"github.com/ovn-org/libovsdb/client"
	"github.com/ovn-org/libovsdb/model"
)

type eventType string

const (
	updateEvent eventType = "UPDATE"
	addEvent    eventType = "ADD"
	deleteEvent eventType = "DELETE"

	sname = "ovsdbShell"
)

type OvsdbEvent struct {
	Timestamp time.Time
	Event     eventType
	Table     string
	Old       model.Model
	New       model.Model
}

type OvsdbShell struct {
	mutex           *sync.RWMutex
	monitor         bool
	ovs             client.Client
	dbModel         *model.DBModel
	events          []OvsdbEvent
	tablesToMonitor []client.TableMonitor
	// Cache metadata used for command autocompletion
	tableFields map[string][]string // holds exact names of fields indexed by table
	indexes     map[string][]string // holds name of the index fields indexed by table
}

func (s *OvsdbShell) Monitor(monitor bool) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.monitor = true
}

func (s *OvsdbShell) printEvent(event OvsdbEvent) {
	dt := time.Now()
	fmt.Printf("New \033[1m%s\033[0m event on table: \033[1m%s\033[0m\n", event.Event, event.Table)
	fmt.Println(dt.Format(time.RFC3339Nano))
	switch event.Event {
	case updateEvent:
		fmt.Println(colordiff(event.Old, event.New))
	case addEvent:
		fmt.Printf("\x1b[32m%s\x1b[0m\n", pretty.CompareConfig.Sprint(event.New))
	case deleteEvent:
		fmt.Printf("\x1b[31m%s\x1b[0m\n", pretty.CompareConfig.Sprint(event.Old))
	}
	fmt.Print("\n")
}

func (s *OvsdbShell) OnAdd(table string, m model.Model) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	if s.monitor {
		event := OvsdbEvent{
			Timestamp: time.Now(),
			Event:     addEvent,
			Table:     table,
			New:       m,
		}
		s.printEvent(event)
		s.events = append(s.events, event)
	}
}

func (s *OvsdbShell) OnUpdate(table string, old, new model.Model) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	if s.monitor {
		event := OvsdbEvent{
			Timestamp: time.Now(),
			Event:     updateEvent,
			Table:     table,
			New:       new,
			Old:       old,
		}
		s.printEvent(event)
		s.events = append(s.events, event)
	}
}

func (s *OvsdbShell) OnDelete(table string, m model.Model) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	if s.monitor {
		event := OvsdbEvent{
			Timestamp: time.Now(),
			Event:     deleteEvent,
			Table:     table,
			Old:       m,
		}
		s.printEvent(event)
		s.events = append(s.events, event)
	}
}

func (s *OvsdbShell) Save(filePath string) error {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	content, err := json.MarshalIndent(s.events, "", "    ")
	if err != nil {
		return err
	}
	return ioutil.WriteFile(filePath, content, 0644)
}

func (s *OvsdbShell) Run(ovs client.Client, args ...string) {
	s.ovs = ovs
	s.ovs.Cache().AddEventHandler(s)

	// if _, err := ovs.MonitorAll(context.Background()); err != nil {
	if _, err := s.ovs.Monitor(context.Background(), s.tablesToMonitor...); err != nil {
		panic(err)
	}

	for tableName, tableSchema := range s.ovs.Schema().Tables {
		indexes := []string{"UUID"}
		for _, idx := range tableSchema.Indexes {
			if len(idx) > 1 {
				// Multi-column index not supported
				continue
			}
			if field, ok := s.exactFieldName(tableName, idx[0]); ok {
				indexes = append(indexes, field)
			}

		}
		s.indexes[tableName] = indexes
	}
	time.Sleep(time.Duration(1<<63 - 1))
}

func newOvsdbShell(auto bool, dbmodel *model.DBModel, tablesToMonitor []client.TableMonitor) *OvsdbShell {
	// Generate the list of columns for each table to be used as command auto-completion options
	tableFields := make(map[string][]string)
	for tname, mtype := range dbmodel.Types() {
		fields := []string{}
		for i := 0; i < mtype.Elem().NumField(); i++ {
			fields = append(fields, mtype.Elem().Field(i).Name)
		}
		tableFields[tname] = fields

	}
	return &OvsdbShell{
		mutex:           new(sync.RWMutex),
		monitor:         auto,
		dbModel:         dbmodel,
		tablesToMonitor: tablesToMonitor,
		tableFields:     tableFields,
		indexes:         make(map[string][]string),
	}
}

// colordiff is similar to what pretty.compare does but with colors
func colordiff(a, b interface{}) string {
	config := pretty.CompareConfig
	alines := strings.Split(config.Sprint(a), "\n")
	blines := strings.Split(config.Sprint(b), "\n")

	buf := new(strings.Builder)
	for _, c := range diff.DiffChunks(alines, blines) {
		for _, line := range c.Added {
			fmt.Fprintf(buf, "\x1b[32m+%s\x1b[0m\n", line)
		}
		for _, line := range c.Deleted {
			fmt.Fprintf(buf, "\x1b[31m-%s\x1b[0m\n", line)
		}
		for _, line := range c.Equal {
			fmt.Fprintf(buf, " %s\n", line)
		}
	}
	return strings.TrimRight(buf.String(), "\n")
}

// filterAPI returns the conditional API that filters based on the provided filter expression
// Expression is [FIELD]=[VALUE]
func (s *OvsdbShell) filterAPI(tableName string, expr string) (client.ConditionalAPI, error) {
	condModel, err := s.dbModel.NewModel(tableName)
	if err != nil {
		return nil, err
	}
	parts := strings.Split(expr, "=")
	if len(parts) != 2 {
		return nil, fmt.Errorf("Invalid filter expression: %s. Use: [FIELD]=[VALUE]", expr)
	}

	field := parts[0]
	value := parts[1]

	fieldName, present := s.exactFieldName(tableName, field)
	if !present {
		return nil, fmt.Errorf("field %s not present in database table %s", field, tableName)
	}

	fieldVal := reflect.ValueOf(condModel).Elem().FieldByName(fieldName)
	if fieldVal.Kind() != reflect.String {
		return nil, fmt.Errorf("filters only support string values")
	}

	fieldVal.Set(reflect.ValueOf(value))

	return s.ovs.Where(condModel), nil
}

// listAutoComplete returns the list of strings to use for list command auto-completion
func (s *OvsdbShell) listAutoComplete(tableName string, prefix string, args []string) []string {
	autocomplete := []string{}
	// Find where "--filter" is in the args
	filterIndex := -1
	for i, arg := range args {
		if arg == "--filter" {
			filterIndex = i
			break
		}
	}
	switch filterIndex {
	case -1: // --filter has not been used yet, offer it
		autocomplete = append(s.tableFields[tableName], "--filter")
	case len(args) - 1: // We're autocompeting a filter, return the indexes and append "="
		for _, f := range s.indexes[tableName] {
			autocomplete = append(autocomplete, f+"=")
		}
	default: // filter has already been processed
		autocomplete = s.tableFields[tableName]
	}
	if prefix != "" {
		autocomplete = addLower(autocomplete)
	}
	return autocomplete
}

// exactFieldName returns the exact field name based on a field name that might have different capitalization
func (s *OvsdbShell) exactFieldName(tableName string, field string) (string, bool) {
	stype := s.dbModel.Types()[tableName].Elem()
	for i := 0; i < stype.NumField(); i++ {
		if strings.EqualFold(stype.Field(i).Name, field) {
			return stype.Field(i).Name, true
		}
	}
	return "", false
}

// addLower expands the given slice of fields with their lower case alternatives
func addLower(fields []string) []string {
	for _, field := range fields {
		lower := strings.ToLower(field)
		if lower != field {
			fields = append(fields, lower)
		}
	}
	return fields
}
