package server

import (
	"bytes"
	"os"
	"path/filepath"
	"sort"
	"strconv"

	"github.com/signintech/gopdf"
	"github.com/znbasedb/znbase/pkg/roachpb"
	"github.com/znbasedb/znbase/pkg/server/diagnosticspb"
	"github.com/znbasedb/znbase/pkg/server/serverpb"
	"github.com/znbasedb/znbase/pkg/ttfdata"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
)

//NodeMetricOutPut is to set a map for default node metrics
type NodeMetricOutPut map[string]string

//StoreMetricOutPut is to set a map for default store metrics
type StoreMetricOutPut map[string]string

//DataFilePath store the cluster store path like "znbase-data"
var DataFilePath string

//defaultReportNodeMetrics add the default nodes metrics for reports
func defaultReportNodeMetrics() NodeMetricOutPut {
	defaultRM := NodeMetricOutPut{
		"cpu系统使用百分比:": "sys.cpu.sys.percent",
		"cpu用户使用百分比:": "sys.cpu.user.percent",
		"cpu用户总时间:":   "sys.cpu.user.ns",
		"cpu系统总时间:":   "sys.cpu.sys.ns",
		"磁盘读取字节数:":    "sys.host.disk.read.bytes",
		"磁盘写入字节数:":    "sys.host.disk.write.bytes",
		"磁盘使用时间:":     "sys.host.disk.io.time",
		"网络接收字节数:":    "sys.host.net.recv.bytes",
		"网络发送字节数:":    "sys.host.net.send.bytes",
		"运行时间:":       "sys.uptime",
	}
	return defaultRM
}

//defaultReportStoreMetrics add the default store metrics for reports
func defaultReportStoreMetrics() StoreMetricOutPut {
	defaultRM := StoreMetricOutPut{
		"磁盘容量:":    "capacity",
		"已使用磁盘容量:": "capacity.used",
		"可用磁盘容量:":  "capacity.available",
	}
	return defaultRM
}

//NodePdfExporter export the raw node info for node metrics and store info
func NodePdfExporter(
	nodes *serverpb.NodesResponse,
	diagnostics []*diagnosticspb.DiagnosticReport,
	problemNodeID map[int]struct{},
	nodestatus map[roachpb.NodeID]NodeStatusWithLiveness,
) {
	//initialize the report pdf
	pdf := gopdf.GoPdf{}
	pdf.Start(gopdf.Config{PageSize: *gopdf.PageSizeA4})
	pdf.AddPage()
	res, _ := ttfdata.Asset("microsoft.ttf")
	rd := bytes.NewReader(res)
	err := pdf.AddTTFFontByReaderWithOption("microsoft", rd, gopdf.TtfOption{UseKerning: false, Style: 0})
	if err != nil {
		return
	}

	//pdf style settings
	err = pdf.SetFont("microsoft", "", 14)
	if err != nil {
		return
	}
	nodeNum := len(nodes.Nodes)
	//get node metric info
	nodeMetricInfo := defaultReportNodeMetrics()
	sortNodeInfo := sortMetricName(nodeMetricInfo)
	//get store metric info
	storeMetricInfo := defaultReportStoreMetrics()
	sortStoreInfo := sortMetricName(storeMetricInfo)

	for i := 0; i < nodeNum; i++ {
		if _, ok := problemNodeID[i]; ok {
			_ = pdf.Cell(nil, "节点"+strconv.FormatInt(int64(i+1), 10)+"存在问题无法导出该节点报表")
			pdf.Br(20)
			_ = pdf.Cell(nil, "节点状态:"+getNodeStatus(nodestatus, nodes, i))
		} else {
			_ = pdf.Cell(nil, "以下为当前节点metrics")
			pdf.Br(20)
			_ = pdf.Cell(nil, "NodeID:"+nodes.Nodes[i].Desc.NodeID.String())
			pdf.Br(20)
			for _, metricName := range sortNodeInfo {
				_ = pdf.Cell(nil, metricName+strconv.FormatFloat(nodes.Nodes[i].Metrics[nodeMetricInfo[metricName]], 'f', -1, 32))
				pdf.Br(20)
			}
			_ = pdf.Cell(nil, "以下为当前节点存储metrics")
			pdf.Br(20)
			for _, metricName := range sortStoreInfo {
				_ = pdf.Cell(nil, metricName+strconv.FormatFloat(nodes.Nodes[i].StoreStatuses[0].Metrics[storeMetricInfo[metricName]], 'f', -1, 32))
				pdf.Br(20)
			}
			_ = pdf.Cell(nil, "当前节点可用内存:"+strconv.FormatInt(int64(diagnostics[i].Node.Hardware.Mem.Available), 10))
			pdf.Br(20)
			_ = pdf.Cell(nil, "当前节点总内存:"+strconv.FormatInt(int64(diagnostics[i].Node.Hardware.Mem.Total), 10))
			pdf.Br(20)
		}
		if i != nodeNum-1 {
			pdf.AddPage()
		}
	}
	//get format time stamp
	timeUnix := timeutil.Now().Unix()

	formatTimeStr := timeutil.Unix(timeUnix, 0).Format("2006-01-02 15:04:05")

	pdfPath := createDir() + "/"

	_ = pdf.WritePdf(pdfPath + "节点信息报表" + formatTimeStr + ".pdf")
}

//sortMetricName sort the name by string in map for reports
func sortMetricName(m map[string]string) []string {
	var ks []string
	for k := range m {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	return ks
}

//createDIr is use to make the file path folders for reports
func createDir() string {
	dir := DataFilePath

	//create main report path folder
	folderPath := filepath.Join(dir, "/reports/")
	if _, err := os.Stat(folderPath); os.IsNotExist(err) {
		_ = os.Mkdir(folderPath, 0777)
		_ = os.Chmod(folderPath, 0777)
	}

	//create folders of the date to simply sort the reports of different days
	folderNameTime := timeutil.Now().Format("2006-01-02")
	folderPath = filepath.Join(folderPath, folderNameTime)
	if _, err := os.Stat(folderPath); os.IsNotExist(err) {
		_ = os.Mkdir(folderPath, 0777)
		_ = os.Chmod(folderPath, 0777)
	}
	return folderPath
}

//GetReportPath return the reports path
func GetReportPath() string {
	dir := DataFilePath
	return dir + "/reports"
}

//GetDataPath get the cluster data store path
func GetDataPath(s *Server) {
	DataFilePath = s.cfg.Stores.Specs[0].Path
}

//SQLPdfExporter export sql raw info to a report pdf
func SQLPdfExporter(stat []roachpb.CollectedStatementStatistics) {
	pdf := gopdf.GoPdf{}
	pdf.Start(gopdf.Config{PageSize: *gopdf.PageSizeLedger})
	pdf.AddPage()
	res, _ := ttfdata.Asset("microsoft.ttf")
	rd := bytes.NewReader(res)
	err := pdf.AddTTFFontByReaderWithOption("microsoft", rd, gopdf.TtfOption{UseKerning: false, Style: 0})
	if err != nil {
		return
	}

	//pdf style settings
	err = pdf.SetFont("microsoft", "", 14)
	if err != nil {
		return
	}

	for i := 0; i < len(stat); i++ {
		_ = pdf.Cell(nil, "sql语句:"+stat[i].Key.Query)
		pdf.Br(20)

		if stat[i].Key.Failed {
			_ = pdf.Cell(nil, "sql执行结果:失败")
		} else {
			_ = pdf.Cell(nil, "sql执行结果:成功")
		}
		pdf.Br(20)

		_ = pdf.Cell(nil, "sql执行次数:"+strconv.FormatInt(stat[i].Stats.Count, 10))
		pdf.Br(20)

		_ = pdf.Cell(nil, "最大重试次数:"+strconv.FormatInt(stat[i].Stats.MaxRetries, 10))
		pdf.Br(20)

		_ = pdf.Cell(nil, "到首次执行成功的重试次数:"+strconv.FormatInt(stat[i].Stats.FirstAttemptCount, 10))
		pdf.Br(20)

		_ = pdf.Cell(nil, "影响行数:"+strconv.FormatFloat(stat[i].Stats.NumRows.Mean, 'f', -1, 32))
		pdf.Br(20)

		_ = pdf.Cell(nil, "以下为当前语句的逻辑计划:")
		pdf.Br(20)

		_ = pdf.Cell(nil, "名称:"+stat[i].Stats.SensitiveInfo.MostRecentPlanDescription.Name)
		pdf.Br(20)

		_ = pdf.Cell(nil, "属性:")
		pdf.Br(20)

		for _, v := range stat[i].Stats.SensitiveInfo.MostRecentPlanDescription.Attrs {
			_ = pdf.Cell(nil, v.Key+"="+v.Value)
			pdf.Br(20)
		}

		//a children plan may contain children plan,so need to judge the children whether nil
		if stat[i].Stats.SensitiveInfo.MostRecentPlanDescription.Children != nil {
			_ = pdf.Cell(nil, "子计划:")
			pdf.Br(20)
			getChildrenInfo(stat[i].Stats.SensitiveInfo.MostRecentPlanDescription.Children, &pdf)
		}
		if i != len(stat)-1 {
			pdf.AddPage()
		}
	}

	timeUnix := timeutil.Now().Unix() //已知的时间戳

	formatTimeStr := timeutil.Unix(timeUnix, 0).Format("2006-01-02 15:04:05")

	pdfPath := createDir() + "/"

	_ = pdf.WritePdf(pdfPath + "SQL语句信息报表" + formatTimeStr + ".pdf")

}

//getChildrenInfo range the sql statement's children plan and write to the pdf
func getChildrenInfo(Children []*roachpb.ExplainTreePlanNode, pdf *gopdf.GoPdf) {
	for i := 0; i < len(Children); i++ {
		_ = pdf.Cell(nil, "子计划名称:"+Children[i].Name)
		pdf.Br(20)

		_ = pdf.Cell(nil, "子计划属性:")
		pdf.Br(20)

		for _, v := range Children[i].Attrs {
			_ = pdf.Cell(nil, v.Key+"="+v.Value)
			pdf.Br(20)
		}
		if Children[i].Children == nil {
			break
		} else {
			_ = pdf.Cell(nil, "下一子计划")
			pdf.Br(20)
			getChildrenInfo(Children[i].Children, pdf)
		}
	}
}

//getNodeMetrics return the sum and average of a node metrics
func getNodeMetrics(
	metricName string, nodeNum int, problemNodeID map[int]struct{}, nodes *serverpb.NodesResponse,
) (float64, float64) {
	var metricSum float64
	//need to calculate the normal node's metrics
	actNodeNum := float64(nodeNum - len(problemNodeID))
	for i := 0; i < nodeNum; i++ {
		if _, ok := problemNodeID[i]; ok {
			continue
		} else {
			metricSum = metricSum + nodes.Nodes[i].Metrics[metricName]
		}
	}
	return metricSum, metricSum / actNodeNum
}

//getStoreMetrics return the sum and average of a store metrics
func getStoreMetrics(
	metricName string, nodeNum int, problemNodeID map[int]struct{}, nodes *serverpb.NodesResponse,
) (float64, float64) {
	var metricSum float64
	//need to calculate the normal node's metrics
	actNodeNum := float64(nodeNum - len(problemNodeID))
	for i := 0; i < nodeNum; i++ {
		if _, ok := problemNodeID[i]; ok {
			continue
		} else {
			metricSum = metricSum + nodes.Nodes[i].StoreStatuses[0].Metrics[metricName]
		}
	}
	return metricSum, metricSum / actNodeNum
}

//getNodeStatus return the node status for reports
func getNodeStatus(
	nodeStatus map[roachpb.NodeID]NodeStatusWithLiveness, nodes *serverpb.NodesResponse, nodeID int,
) string {
	var res string
	if nodeStatus[nodes.Nodes[nodeID].Desc.NodeID].LivenessStatus == 0 {
		res = "UNKNOWN"
	} else if nodeStatus[nodes.Nodes[nodeID].Desc.NodeID].LivenessStatus == 1 {
		res = "DEAD"
	} else if nodeStatus[nodes.Nodes[nodeID].Desc.NodeID].LivenessStatus == 2 {
		res = "UNAVAILABLE"
	} else if nodeStatus[nodes.Nodes[nodeID].Desc.NodeID].LivenessStatus == 3 {
		res = "LIVE"
	} else if nodeStatus[nodes.Nodes[nodeID].Desc.NodeID].LivenessStatus == 4 {
		res = "DECOMMISSIONING"
	} else if nodeStatus[nodes.Nodes[nodeID].Desc.NodeID].LivenessStatus == 5 {
		res = "DECOMMISSIONED"
	}
	return res
}

//NodePdfExporterByTable write node metrics and store info into one report table
func NodePdfExporterByTable(
	nodes *serverpb.NodesResponse,
	diagnostics []*diagnosticspb.DiagnosticReport,
	problemNodeID map[int]struct{},
	nodeStatus map[roachpb.NodeID]NodeStatusWithLiveness,
) {
	//initialize the report pdf
	pdf := gopdf.GoPdf{}
	pdf.Start(gopdf.Config{PageSize: *gopdf.PageSizeLedger})
	pdf.AddPage()
	res, _ := ttfdata.Asset("microsoft.ttf")
	rd := bytes.NewReader(res)
	err := pdf.AddTTFFontByReaderWithOption("microsoft", rd, gopdf.TtfOption{UseKerning: false, Style: 0})
	if err != nil {
		return
	}

	//pdf style settings
	err = pdf.SetFont("microsoft", "", 14)
	if err != nil {
		return
	}
	nodeNum := len(nodes.Nodes)
	//report pdf table line set
	pdf.SetLineWidth(0.5)
	pdf.SetLineType("normal")

	//set the words' coordinate
	pdf.SetX(500)
	pdf.SetY(40)
	_ = pdf.Text("数据库性能指标报表")

	pdf.Br(30)
	//draw the line of the table top
	pdf.Line(10, 60, 1210, 60)

	var x, y float64
	x = 10 //left init
	y = 60 //top init
	for i := 0; i < 17; i++ {
		pdf.Line(x, y, x, y+30) //the gap is 30 in y
		x = x + 75              //the gap is 75 in x
		//if one table row is finished,draw a bottom line
		if i == 16 {
			pdf.Line(10, y+30, 1210, y+30)
		}
	}

	_ = pdf.SetFont("microsoft", "", 8)

	pdf.SetX(10)
	pdf.SetY(71)
	_ = pdf.Cell(nil, "对象名称")
	pdf.SetX(85)
	_ = pdf.Cell(nil, "节点状态")
	pdf.SetX(160)
	_ = pdf.Cell(nil, "系统cpu使用百分比")
	pdf.SetX(235)
	_ = pdf.Cell(nil, "用户cpu使用百分比")
	pdf.SetX(310)
	_ = pdf.Cell(nil, "系统cpu总时间")
	pdf.SetX(385)
	_ = pdf.Cell(nil, "用户cpu总时间")
	pdf.SetX(460)
	_ = pdf.Cell(nil, "总磁盘容量")
	pdf.SetX(535)
	_ = pdf.Cell(nil, "可用磁盘容量")
	pdf.SetX(610)
	_ = pdf.Cell(nil, "已使用磁盘容量")
	pdf.SetX(685)
	_ = pdf.Cell(nil, "磁盘读取")
	pdf.SetX(760)
	_ = pdf.Cell(nil, "磁盘写入")
	pdf.SetX(835)
	_ = pdf.Cell(nil, "网络接收")
	pdf.SetX(910)
	_ = pdf.Cell(nil, "网络发送")
	pdf.SetX(985)
	_ = pdf.Cell(nil, "总内存")
	pdf.SetX(1060)
	_ = pdf.Cell(nil, "可用内存")
	pdf.SetX(1135)
	_ = pdf.Cell(nil, "运行时间")

	x = 10
	y = 90
	for i := 0; i < 17; i++ {
		pdf.Line(x, y, x, y+30)
		x = x + 75
		if i == 16 {
			pdf.Line(10, y+30, 1210, y+30)
		}
	}

	pdf.SetX(10)
	pdf.SetY(101)
	_ = pdf.Cell(nil, "集群")
	pdf.SetX(85)
	_ = pdf.Cell(nil, "/")

	pdf.SetX(160)
	_, avg := getNodeMetrics("sys.cpu.sys.percent", nodeNum, problemNodeID, nodes)
	_ = pdf.Cell(nil, strconv.FormatFloat(avg*100, 'f', 4, 64)+"%")
	pdf.SetX(235)
	_, avg = getNodeMetrics("sys.cpu.user.percent", nodeNum, problemNodeID, nodes)
	_ = pdf.Cell(nil, strconv.FormatFloat(avg*100, 'f', 4, 64)+"%")
	pdf.SetX(310)
	_, avg = getNodeMetrics("sys.cpu.user.ns", nodeNum, problemNodeID, nodes)
	_ = pdf.Cell(nil, strconv.FormatFloat(avg/1000000000, 'f', 2, 64)+"s")
	pdf.SetX(385)
	_, avg = getNodeMetrics("sys.cpu.sys.ns", nodeNum, problemNodeID, nodes)
	_ = pdf.Cell(nil, strconv.FormatFloat(avg/1000000000, 'f', 2, 64)+"s")

	pdf.SetX(460)
	sum, _ := getStoreMetrics("capacity", nodeNum, problemNodeID, nodes)
	_ = pdf.Cell(nil, strconv.FormatFloat(sum/1073741824, 'f', 2, 64)+"GB")
	pdf.SetX(535)
	sum, _ = getStoreMetrics("capacity.available", nodeNum, problemNodeID, nodes)
	_ = pdf.Cell(nil, strconv.FormatFloat(sum/1073741824, 'f', 2, 64)+"GB")
	pdf.SetX(610)
	sum, _ = getStoreMetrics("capacity.used", nodeNum, problemNodeID, nodes)
	_ = pdf.Cell(nil, strconv.FormatFloat(sum/1073741824, 'f', 2, 64)+"GB")

	pdf.SetX(685)
	sum, _ = getNodeMetrics("sys.host.disk.read.bytes", nodeNum, problemNodeID, nodes)
	_ = pdf.Cell(nil, strconv.FormatFloat(sum/1048576, 'f', 2, 64)+"MB")
	pdf.SetX(760)
	sum, _ = getNodeMetrics("sys.host.net.recv.bytes", nodeNum, problemNodeID, nodes)
	_ = pdf.Cell(nil, strconv.FormatFloat(sum/1048576, 'f', 2, 64)+"MB")

	pdf.SetX(835)
	sum, _ = getNodeMetrics("sys.host.net.recv.bytes", nodeNum, problemNodeID, nodes)
	_ = pdf.Cell(nil, strconv.FormatFloat(sum/1048576, 'f', 2, 64)+"MB")
	pdf.SetX(910)
	sum, _ = getNodeMetrics("sys.host.net.send.bytes", nodeNum, problemNodeID, nodes)
	_ = pdf.Cell(nil, strconv.FormatFloat(sum/1048576, 'f', 2, 64)+"MB")

	pdf.SetX(985)
	totalSum, _ := getDiaMetrics(nodeNum, problemNodeID, diagnostics)
	_ = pdf.Cell(nil, strconv.FormatFloat(totalSum/1048576, 'f', 2, 64)+"MB")
	pdf.SetX(1060)
	_, avaSum := getDiaMetrics(nodeNum, problemNodeID, diagnostics)
	_ = pdf.Cell(nil, strconv.FormatFloat(avaSum/1048676, 'f', 2, 64)+"MB")

	pdf.SetX(1135)
	_, avg = getNodeMetrics("sys.uptime", nodeNum, problemNodeID, nodes)
	_ = pdf.Cell(nil, strconv.FormatFloat(avg, 'f', -1, 64)+"s")

	for i := 0; i < nodeNum; i++ {
		x = 10
		y = y + 30
		for i := 0; i < 17; i++ {
			pdf.Line(x, y, x, y+30)
			x = x + 75
			if i == 16 {
				pdf.Line(10, y+30, 1210, y+30)
			}
		}

		pdf.SetX(10)
		pdf.SetY(y + 11)
		_ = pdf.Cell(nil, strconv.FormatInt(int64(nodes.Nodes[i].Desc.NodeID), 10))

		if _, ok := problemNodeID[i]; ok {
			pdf.SetX(85)
			_ = pdf.Cell(nil, getNodeStatus(nodeStatus, nodes, i))
		} else {

			pdf.SetX(85)
			_ = pdf.Cell(nil, getNodeStatus(nodeStatus, nodes, i))

			pdf.SetX(160)
			_ = pdf.Cell(nil, strconv.FormatFloat(nodes.Nodes[i].Metrics["sys.cpu.sys.percent"]*100, 'f', 4, 64)+"%")
			pdf.SetX(235)
			_ = pdf.Cell(nil, strconv.FormatFloat(nodes.Nodes[i].Metrics["sys.cpu.user.percent"]*100, 'f', 4, 64)+"%")
			pdf.SetX(310)
			_ = pdf.Cell(nil, strconv.FormatFloat(nodes.Nodes[i].Metrics["sys.cpu.user.ns"]/1000000000, 'f', 2, 64)+"s")
			pdf.SetX(385)
			_ = pdf.Cell(nil, strconv.FormatFloat(nodes.Nodes[i].Metrics["sys.cpu.sys.ns"]/1000000000, 'f', 2, 64)+"s")

			pdf.SetX(460)
			_ = pdf.Cell(nil, strconv.FormatFloat(nodes.Nodes[i].StoreStatuses[0].Metrics["capacity"]/1073741824, 'f', 2, 64)+"GB")
			pdf.SetX(535)
			_ = pdf.Cell(nil, strconv.FormatFloat(nodes.Nodes[i].StoreStatuses[0].Metrics["capacity.available"]/1073741824, 'f', 2, 64)+"GB")
			pdf.SetX(610)
			_ = pdf.Cell(nil, strconv.FormatFloat(nodes.Nodes[i].StoreStatuses[0].Metrics["capacity.used"]/1073741824, 'f', 2, 64)+"GB")

			pdf.SetX(685)
			_ = pdf.Cell(nil, strconv.FormatFloat(nodes.Nodes[i].Metrics["sys.host.disk.read.bytes"]/1048576, 'f', 2, 64)+"MB")
			pdf.SetX(760)
			_ = pdf.Cell(nil, strconv.FormatFloat(nodes.Nodes[i].Metrics["sys.host.net.recv.bytes"]/1048576, 'f', 2, 64)+"MB")

			pdf.SetX(835)
			_ = pdf.Cell(nil, strconv.FormatFloat(nodes.Nodes[i].Metrics["sys.host.net.recv.bytes"]/1048576, 'f', 2, 64)+"MB")
			pdf.SetX(910)
			_ = pdf.Cell(nil, strconv.FormatFloat(nodes.Nodes[i].Metrics["sys.host.net.send.bytes"]/1048576, 'f', 2, 64)+"MB")

			pdf.SetX(985)
			_ = pdf.Cell(nil, strconv.FormatFloat(float64(diagnostics[i].Node.Hardware.Mem.Available/1048676), 'f', 2, 64)+"MB")
			pdf.SetX(1060)
			_ = pdf.Cell(nil, strconv.FormatFloat(float64(diagnostics[i].Node.Hardware.Mem.Total/1048576), 'f', 2, 64)+"MB")

			pdf.SetX(1135)
			_ = pdf.Cell(nil, strconv.FormatFloat(nodes.Nodes[i].Metrics["sys.uptime"], 'f', -1, 64)+"s")

		}
		//if y>720, add a new pdf page,and reInit y
		if y > 720 {
			pdf.AddPage()
			y = 90
		}
	}

	//get format time stamp
	timeUnix := timeutil.Now().Unix()

	formatTimeStr := timeutil.Unix(timeUnix, 0).Format("2006-01-02 15:04:05")

	pdfPath := createDir() + "/"

	_ = pdf.WritePdf(pdfPath + "节点信息报表表格" + formatTimeStr + ".pdf")
}

//getDiaMetrics get the memory metrics from DiagnosticReport
func getDiaMetrics(
	nodeNum int, problemNodeID map[int]struct{}, diagnostics []*diagnosticspb.DiagnosticReport,
) (float64, float64) {
	var avaSum, totalSum float64
	for i := 0; i < nodeNum; i++ {
		if _, ok := problemNodeID[i]; ok {
			continue
		} else {
			avaSum += float64(diagnostics[i].Node.Hardware.Mem.Available)
			totalSum += float64(diagnostics[i].Node.Hardware.Mem.Total)
		}
	}
	return avaSum, totalSum
}
