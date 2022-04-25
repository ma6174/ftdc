package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"sort"
	"strings"
	"time"

	"github.com/simagix/keyhole/ftdc"
)

// ftdc -mongostat dir|file

var mongostatMetrics = cleanMetrics(`
serverStatus/opcounters/insert+serverStatus/opcountersRepl/insert,insert,d;
serverStatus/opcounters/query+serverStatus/opcountersRepl/query,query,d;
serverStatus/opcounters/update+serverStatus/opcountersRepl/update,update,d;
serverStatus/opcounters/delete+serverStatus/opcountersRepl/delete,delete,d;
serverStatus/opcounters/getmore+serverStatus/opcountersRepl/getmore,getmore,d;
serverStatus/opcounters/command+serverStatus/opcountersRepl/command,command,d;
serverStatus/connections/current,conn,;
replSetGetStatus/myState,state,;
serverStatus/mem/resident,res_M,;
serverStatus/mem/virtual,vsize_M,;
serverStatus/uptime,uptime,;
`)

var cpuStatMetrics = cleanMetrics(`
systemMetrics/cpu/btime,btime,d;
systemMetrics/cpu/ctxt,ctxt,d;
systemMetrics/cpu/idle_ms,idle,d;
systemMetrics/cpu/iowait_ms,iowait,d;
systemMetrics/cpu/irq_ms,irq,d;
systemMetrics/cpu/nice_ms,nice,d;
systemMetrics/cpu/procs_running,procs_run,;
systemMetrics/cpu/softirq_ms,softirq,d;
systemMetrics/cpu/steal_ms,steal_ms,d;
systemMetrics/cpu/system_ms,system,d;
systemMetrics/cpu/user_ms,user,d;
`)

var memStatMetrics = cleanMetrics(`
systemMetrics/memory/Active_kb,Active,;
systemMetrics/memory/Buffers_kb,Buffers,;
systemMetrics/memory/Cached_kb,Cached,;
systemMetrics/memory/Dirty_kb,Dirty,;
systemMetrics/memory/Inactive_kb,Inactive,;
systemMetrics/memory/MemFree_kb,MemFree,;
systemMetrics/memory/MemTotal_kb,MemTotal,;
systemMetrics/memory/SwapCached_kb,SwapCached,;
systemMetrics/memory/SwapFree_kb,SwapFree,;
systemMetrics/memory/SwapTotal_kb,SwapTotal,;
`)

func main() {
	cmetrics := flag.String("metrics", "", "show custom metrics")
	cpu := flag.Bool("cpu", false, "cpu stats")
	mem := flag.Bool("mem", false, "mem stats")
	width := flag.Int("width", 8, "width")
	keys := flag.Bool("keys", false, "show all keys")
	flag.Parse()
	data, err := ioutil.ReadFile(flag.Arg(0))
	if err != nil {
		log.Panicln(err)
	}
	f := ftdc.NewMetrics()
	err = f.ReadAllMetrics(&data)
	if err != nil {
		log.Panicln(err)
	}
	cm := mongostatMetrics
	if *cpu {
		cm = cpuStatMetrics
	}
	if *mem {
		cm = memStatMetrics
	}
	if *cmetrics != "" {
		cm = *cmetrics
	}
	if *keys {
		var keyList []string
		for key := range f.Data[0].DataPointsMap {
			keyList = append(keyList, key)
		}
		sort.Slice(keyList, func(i, j int) bool { return keyList[i] < keyList[j] })
		for _, key := range keyList {
			fmt.Println(key)
		}
		return
	}
	metrics := strings.Split(cm, ";")
	header := getHeader(metrics, *width)
	fmt.Println(header)
	format := fmt.Sprintf("%%%d.%ds", *width, *width)
	for _, data := range f.Data {
		count := len(data.DataPointsMap["replSetGetStatus/date"])
		for i := 0; i < count; i++ {
			t := int64(data.DataPointsMap["replSetGetStatus/date"][i])
			t1 := time.Unix(t/1000, 0).Format("06-01-02 15:04:05")
			line := t1
			for _, metric := range metrics {
				value := getValue(&data, i, metric)
				line += fmt.Sprintf(format, fmt.Sprint(value))
			}
			if t/1000%10 == 0 {
				fmt.Println(header)
			}
			fmt.Println(line)
		}
	}
}

func cleanMetrics(m string) string {
	for _, v := range []string{" ", "\t", "\n", "\r"} {
		m = strings.ReplaceAll(m, v, "")
	}
	if strings.HasSuffix(m, ";") {
		m = m[:len(m)-1]
	}
	return m
}

func getHeader(metrics []string, width int) (h string) {
	format := fmt.Sprintf("%%%dv", width)
	h += fmt.Sprintf("%-17v", "time")
	for _, metric := range metrics {
		ms := strings.SplitN(metric, ",", 3)
		m := ""
		if len(ms) >= 2 {
			m = ms[1]
		}
		h += fmt.Sprintf(format, m)
	}
	return
}

var last = make(map[string]uint64)

func getValue(data *ftdc.MetricsData, offset int, metric string) (v uint64) {
	sp := strings.SplitN(metric, ",", 3)
	if len(sp) < 3 {
		return 0
	}
	keys, isDelta := strings.Split(sp[0], "+"), sp[2] == "d"
	for _, key := range keys {
		var d uint64
		if len(data.DataPointsMap[key]) > offset {
			d = data.DataPointsMap[key][offset]
		}
		if isDelta {
			if _, ok := last[key]; !ok {
				last[key] = d
			}
			if d == 0 || d < last[key] {
				last[key] = 0
			}
			d, last[key] = d-last[key], d
		}
		v += d
	}
	return
}
