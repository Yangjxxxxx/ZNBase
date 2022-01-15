package ts

import (
	"bytes"
	"os"
	"path/filepath"
	"strconv"

	"github.com/signintech/gopdf"
	"github.com/znbasedb/znbase/pkg/ts/tspb"
	"github.com/znbasedb/znbase/pkg/ttfdata"
	"github.com/znbasedb/znbase/pkg/util/timeutil"
)

//tsetQuery is used to send a short default query to check the number of the source in the
//chosen period
func testQuery(startTime int64, endTime int64) *tspb.TimeSeriesQueryRequest {
	var dfq tspb.TimeSeriesQueryRequest
	dfq.StartNanos = startTime
	dfq.EndNanos = endTime
	dfq.SampleNanos = 10000000000
	dfq.Queries = make([]tspb.Query, 1)
	dfq.Queries[0].Name = "cr.node.sys.cpu.combined.percent-normalized"
	dfq.Queries[0].Downsampler = tspb.TimeSeriesQueryAggregator(1).Enum()

	return &dfq
}

//defaultQuery if used to make the default query list for the reports,may add more query
//or make a new defaultQuery list in the future
func defaultQuery(nodeNum int, startTime int64, endTime int64) *tspb.TimeSeriesQueryRequest {

	//var dfq a simulated TSQRequest to get the report data
	var dfq tspb.TimeSeriesQueryRequest
	dfq.StartNanos = startTime
	dfq.EndNanos = endTime
	dfq.SampleNanos = 10000000000

	//now the default query number is 10,send 10*nodeNum requests,may add new in the feature
	dfq.Queries = make([]tspb.Query, 10*nodeNum)

	for i := 0; i < nodeNum; i++ {

		dfq.Queries[0+i*10].Name = "cr.node.sys.cpu.combined.percent-normalized"
		dfq.Queries[0+i*10].Downsampler = tspb.TimeSeriesQueryAggregator(1).Enum()
		dfq.Queries[0+i*10].SourceAggregator = tspb.TimeSeriesQueryAggregator(2).Enum()
		dfq.Queries[0+i*10].Derivative = tspb.TimeSeriesQueryDerivative(0).Enum()
		dfq.Queries[0+i*10].Sources = append(dfq.Queries[0+i*10].Sources, strconv.FormatInt(int64(i+1), 10))

		dfq.Queries[1+i*10].Name = "cr.node.sys.rss"
		dfq.Queries[1+i*10].Downsampler = tspb.TimeSeriesQueryAggregator(1).Enum()
		dfq.Queries[1+i*10].SourceAggregator = tspb.TimeSeriesQueryAggregator(2).Enum()
		dfq.Queries[1+i*10].Derivative = tspb.TimeSeriesQueryDerivative(0).Enum()
		dfq.Queries[1+i*10].Sources = append(dfq.Queries[1+i*10].Sources, strconv.FormatInt(int64(i+1), 10))

		dfq.Queries[2+i*10].Name = "cr.node.sys.host.disk.read.bytes"
		dfq.Queries[2+i*10].Downsampler = tspb.TimeSeriesQueryAggregator(1).Enum()
		dfq.Queries[2+i*10].SourceAggregator = tspb.TimeSeriesQueryAggregator(2).Enum()
		dfq.Queries[2+i*10].Derivative = tspb.TimeSeriesQueryDerivative(2).Enum()
		dfq.Queries[2+i*10].Sources = append(dfq.Queries[2+i*10].Sources, strconv.FormatInt(int64(i+1), 10))

		dfq.Queries[3+i*10].Name = "cr.node.sys.host.disk.write.bytes"
		dfq.Queries[3+i*10].Downsampler = tspb.TimeSeriesQueryAggregator(1).Enum()
		dfq.Queries[3+i*10].SourceAggregator = tspb.TimeSeriesQueryAggregator(2).Enum()
		dfq.Queries[3+i*10].Derivative = tspb.TimeSeriesQueryDerivative(2).Enum()
		dfq.Queries[3+i*10].Sources = append(dfq.Queries[3+i*10].Sources, strconv.FormatInt(int64(i+1), 10))

		dfq.Queries[4+i*10].Name = "cr.node.sys.host.disk.read.count"
		dfq.Queries[4+i*10].Downsampler = tspb.TimeSeriesQueryAggregator(1).Enum()
		dfq.Queries[4+i*10].SourceAggregator = tspb.TimeSeriesQueryAggregator(2).Enum()
		dfq.Queries[4+i*10].Derivative = tspb.TimeSeriesQueryDerivative(2).Enum()
		dfq.Queries[4+i*10].Sources = append(dfq.Queries[4+i*10].Sources, strconv.FormatInt(int64(i+1), 10))

		dfq.Queries[5+i*10].Name = "cr.node.sys.host.disk.write.count"
		dfq.Queries[5+i*10].Downsampler = tspb.TimeSeriesQueryAggregator(1).Enum()
		dfq.Queries[5+i*10].SourceAggregator = tspb.TimeSeriesQueryAggregator(2).Enum()
		dfq.Queries[5+i*10].Derivative = tspb.TimeSeriesQueryDerivative(2).Enum()
		dfq.Queries[5+i*10].Sources = append(dfq.Queries[5+i*10].Sources, strconv.FormatInt(int64(i+1), 10))

		dfq.Queries[6+i*10].Name = "cr.node.sys.host.disk.iopsinprogress"
		dfq.Queries[6+i*10].Downsampler = tspb.TimeSeriesQueryAggregator(1).Enum()
		dfq.Queries[6+i*10].SourceAggregator = tspb.TimeSeriesQueryAggregator(2).Enum()
		dfq.Queries[6+i*10].Derivative = tspb.TimeSeriesQueryDerivative(0).Enum()
		dfq.Queries[6+i*10].Sources = append(dfq.Queries[6+i*10].Sources, strconv.FormatInt(int64(i+1), 10))

		dfq.Queries[7+i*10].Name = "cr.store.capacity.available"
		dfq.Queries[7+i*10].Downsampler = tspb.TimeSeriesQueryAggregator(1).Enum()
		dfq.Queries[7+i*10].SourceAggregator = tspb.TimeSeriesQueryAggregator(2).Enum()
		dfq.Queries[7+i*10].Derivative = tspb.TimeSeriesQueryDerivative(0).Enum()
		dfq.Queries[7+i*10].Sources = append(dfq.Queries[7+i*10].Sources, strconv.FormatInt(int64(i+1), 10))

		dfq.Queries[8+i*10].Name = "cr.node.sys.host.net.recv.bytes"
		dfq.Queries[8+i*10].Downsampler = tspb.TimeSeriesQueryAggregator(1).Enum()
		dfq.Queries[8+i*10].SourceAggregator = tspb.TimeSeriesQueryAggregator(2).Enum()
		dfq.Queries[8+i*10].Derivative = tspb.TimeSeriesQueryDerivative(2).Enum()
		dfq.Queries[8+i*10].Sources = append(dfq.Queries[8+i*10].Sources, strconv.FormatInt(int64(i+1), 10))

		dfq.Queries[9+i*10].Name = "cr.node.sys.host.net.send.bytes"
		dfq.Queries[9+i*10].Downsampler = tspb.TimeSeriesQueryAggregator(1).Enum()
		dfq.Queries[9+i*10].SourceAggregator = tspb.TimeSeriesQueryAggregator(2).Enum()
		dfq.Queries[9+i*10].Derivative = tspb.TimeSeriesQueryDerivative(2).Enum()
		dfq.Queries[9+i*10].Sources = append(dfq.Queries[9+i*10].Sources, strconv.FormatInt(int64(i+1), 10))
	}

	return &dfq
}

//PdfExporterTs use the result of the TSQueryResponse to write data into a pdf table
func PdfExporterTs(data *tspb.TimeSeriesQueryResponse, sampleNum int, s *Server) error {
	//initialize the report pdf
	pdf := gopdf.GoPdf{}
	pdf.Start(gopdf.Config{PageSize: *gopdf.PageSizeLedger})
	pdf.AddPage()

	res, _ := ttfdata.Asset("microsoft.ttf")
	rd := bytes.NewReader(res)
	err := pdf.AddTTFFontByReaderWithOption("microsoft", rd, gopdf.TtfOption{UseKerning: false, Style: 0})
	if err != nil {
		return err
	}

	//pdf style settings
	err = pdf.SetFont("microsoft", "", 14)
	if err != nil {
		return err
	}

	//report pdf table line set
	pdf.SetLineWidth(0.5)
	pdf.SetLineType("normal")

	//set the words' coordinate
	pdf.SetX(500)
	pdf.SetY(40)
	_ = pdf.Text("数据库性能指标时序报表")

	pdf.Br(30)

	//draw the line of the table top
	pdf.Line(10, 60, 1110, 60)

	var x, y float64
	x = 10 //left init
	y = 60 //top init
	for i := 0; i < 12; i++ {
		pdf.Line(x, y, x, y+30) //the gap is 30 in y
		x = x + 100             //the gap is 100 in x
		//if one table row is finished,draw a bottom line
		if i == 11 {
			pdf.Line(10, y+30, 1110, y+30)
		}
	}

	_ = pdf.SetFont("microsoft", "", 8)
	pdf.SetX(10)
	pdf.SetY(71)
	_ = pdf.Cell(nil, "节点名称")
	pdf.SetX(110)
	_ = pdf.Cell(nil, "CPU 使用率")
	pdf.SetX(210)
	_ = pdf.Cell(nil, "内存使用量")
	pdf.SetX(310)
	_ = pdf.Cell(nil, "硬盘读取")
	pdf.SetX(410)
	_ = pdf.Cell(nil, "硬盘写入")
	pdf.SetX(510)
	_ = pdf.Cell(nil, "硬盘每秒读取次数")
	pdf.SetX(610)
	_ = pdf.Cell(nil, "硬盘每秒写入次数")
	pdf.SetX(710)
	_ = pdf.Cell(nil, "硬盘每秒IO次数")
	pdf.SetX(810)
	_ = pdf.Cell(nil, "可用的硬盘容量")
	pdf.SetX(910)
	_ = pdf.Cell(nil, "网络接收数据")
	pdf.SetX(1010)
	_ = pdf.Cell(nil, "网络发送数据")

	//write the data of response
	for i := 0; i < sampleNum; i++ {
		x = 10
		y = y + 30
		for i := 0; i < 12; i++ {
			pdf.Line(x, y, x, y+30)
			x = x + 100
			if i == 11 {
				pdf.Line(10, y+30, 1110, y+30)
			}
		}

		var avgValue float64
		pdf.SetX(10)
		pdf.SetY(y + 11)
		_ = pdf.Cell(nil, data.Results[i*10].Sources[0])

		pdf.SetX(110)
		sum := 0.0
		for _, v := range data.Results[0+i*10].Datapoints {
			sum += v.Value
		}
		avgValue = sum / float64(len(data.Results[0+i*10].Datapoints))
		_ = pdf.Cell(nil, strconv.FormatFloat(avgValue*100, 'f', 4, 64)+"%")

		pdf.SetX(210)
		sum = 0
		for _, v := range data.Results[1+i*10].Datapoints {
			sum += v.Value
		}
		avgValue = sum / float64(len(data.Results[1+i*10].Datapoints))
		_ = pdf.Cell(nil, strconv.FormatFloat(avgValue/1048576, 'f', 2, 64)+"MB")

		pdf.SetX(310)
		sum = 0
		for _, v := range data.Results[2+i*10].Datapoints {
			sum += v.Value
		}
		avgValue = sum / float64(len(data.Results[2+i*10].Datapoints))
		_ = pdf.Cell(nil, strconv.FormatFloat(avgValue/1024, 'f', 2, 64)+"KB")

		pdf.SetX(410)
		sum = 0
		for _, v := range data.Results[3+i*10].Datapoints {
			sum += v.Value
		}
		avgValue = sum / float64(len(data.Results[3+i*10].Datapoints))
		_ = pdf.Cell(nil, strconv.FormatFloat(avgValue/1024, 'f', 2, 64)+"KB")

		pdf.SetX(510)
		sum = 0
		for _, v := range data.Results[4+i*10].Datapoints {
			sum += v.Value
		}
		avgValue = sum / float64(len(data.Results[4+i*10].Datapoints))
		_ = pdf.Cell(nil, strconv.FormatFloat(avgValue, 'f', 2, 64))

		pdf.SetX(610)
		sum = 0
		for _, v := range data.Results[5+i*10].Datapoints {
			sum += v.Value
		}
		avgValue = sum / float64(len(data.Results[5+i*10].Datapoints))
		_ = pdf.Cell(nil, strconv.FormatFloat(avgValue, 'f', 2, 64))

		pdf.SetX(710)
		sum = 0
		for _, v := range data.Results[6+i*10].Datapoints {
			sum += v.Value
		}
		avgValue = sum / float64(len(data.Results[6+i*10].Datapoints))
		_ = pdf.Cell(nil, strconv.FormatFloat(avgValue, 'f', 2, 64))

		pdf.SetX(810)
		sum = 0
		for _, v := range data.Results[7+i*10].Datapoints {
			sum += v.Value
		}
		avgValue = sum / float64(len(data.Results[7+i*10].Datapoints))
		_ = pdf.Cell(nil, strconv.FormatFloat(avgValue/1073741824, 'f', 2, 64)+"GB")

		pdf.SetX(910)
		sum = 0
		for _, v := range data.Results[8+i*10].Datapoints {
			sum += v.Value
		}
		avgValue = sum / float64(len(data.Results[8+i*10].Datapoints))
		_ = pdf.Cell(nil, strconv.FormatFloat(avgValue/1024, 'f', 2, 64)+"KB")

		pdf.SetX(1010)
		sum = 0
		for _, v := range data.Results[9+i*10].Datapoints {
			sum += v.Value
		}
		avgValue = sum / float64(len(data.Results[9+i*10].Datapoints))
		_ = pdf.Cell(nil, strconv.FormatFloat(avgValue/1024, 'f', 2, 64)+"KB")

		//if y>720, add a new pdf page,and reInit y
		if y > 720 {
			pdf.AddPage()
			y = 90
		}

	}

	//get the timestamps of now to use in the report name
	timeUnix := timeutil.Now().Unix() //已知的时间戳

	formatTimeStr := timeutil.Unix(timeUnix, 0).Format("2006-01-02 15:04:05")

	pdfPath := createDir(s) + "/"

	_ = pdf.WritePdf(pdfPath + "节点时序报表" + formatTimeStr + ".pdf")

	return nil
}

//createDIr is use to make the file path folders for reports
func createDir(s *Server) string {
	//the ts.Server now do not have a store path config,temp use the ExternalIODir's
	//dir and delete the "/external" in this dir string for data path
	dir := s.storePath

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
