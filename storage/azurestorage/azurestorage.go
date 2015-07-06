package azurestorage

import (
	info "github.com/google/cadvisor/info/v1"
	"github.com/qingfuw/azure-sdk-for-go/storage"
	"time"
	"reflect"
	"strings"
	"encoding/json"
	"strconv"
	"errors"
)

type azurestorage struct{
	table storage.Table
	machine_name string
	dataChan chan AzureRow
}
func New( accountName string, accountKey string , tableName string, machine_name string)(*azurestorage){
	s:= azurestorage{}
	s.table,_ = storage.CreateTable(accountName,accountKey,tableName)
	s.dataChan = make (chan AzureRow)
	s.machine_name = machine_name
	return &s
}
func (self *azurestorage) Close() error {
	
	return nil
}

type AzureRow struct{
	PartitionKey	string	
	RowKey	string	
	Value	float64	
	CounterName	string		
}

func getValue(stats info.ContainerStats,p []string) (float64,string){
	var v = reflect.ValueOf(stats)
	for _,i:=range p{
		if !v.IsValid(){
			return 0,"can't get property "+strings.Join(p,".")
		}
		v=v.FieldByName(i)
	}
	switch v.Kind(){
		case reflect.Int32:
			return float64(v.Int()),""
		case reflect.Uint64:
			return float64(v.Uint()),""
		default:
			return 0,("Field "+strings.Join(p,".")+" type"+string(v.Kind())+" unknow")
	}
	return 0,""
}

func (self *azurestorage) AddStats(ref info.ContainerReference, stats *info.ContainerStats) error {
	if stats == nil {
		return nil
	}

	for _,p:= range([]string{"Cpu.LoadAverage","Cpu.Usage.Total","Memory.ContainerData.Pgfault","Memory.ContainerData.Pgmajfault"}){
		var err string
		row:= AzureRow{}
		row.PartitionKey=self.machine_name
		row.RowKey=strconv.FormatUint(uint64(time.Now().Unix()),10)+"_"+ref.Name+p
		row.Value,err = getValue(info.ContainerStats{},strings.Split(p,"."))
		if len(err) >0 {
			return errors.New(err)
		}
		row.CounterName = p
		self.dataChan<-row
	}
	
	
	return nil;
}

func (self *azurestorage) uploadData()  {
	var batch = make([][]byte,100)
	var flush = time.Tick(time.Second*5)
	for{
		select{
			case _=<-flush:
				if len(batch)>0{
					self.table.Insert(batch)
				}			
				break
			case row:=<-self.dataChan:
				d,_:=json.Marshal(row)
				batch=append(batch,d)
				if len(batch)>=100{
					self.table.Insert(batch)
				}
		}
	}	
} 



