package gziputil

import (
	"bytes"
	"compress/gzip"
	"strings"

	"github.com/znbasedb/znbase/pkg/sql/distsqlpb"
	"github.com/znbasedb/znbase/pkg/util/encoding/csv"
	"github.com/znbasedb/znbase/pkg/util/encoding/txt"
)

const exportFilePatternPart = "%part%"
const exportFilePatternDefault = exportFilePatternPart + ".csv"
const exportFilePatternDefaultTXT = exportFilePatternPart + ".txt"

type csvExporter struct {
	compressor *gzip.Writer
	buf        *bytes.Buffer
	csvWriter  *csv.Writer
}

// Write append record to csv file
// Unused
//func (c *csvExporter) Write(record []string) error {
//	return c.csvWriter.Write(record)
//}

func (c *csvExporter) WriteBytes(p []byte) (int, error) {
	return c.csvWriter.WriteBytes(p)
}

// Close closes the compressor writer which
// appends archive footers
func (c *csvExporter) Close() error {
	if c.compressor != nil {
		return c.compressor.Close()
	}
	return nil
}

// Flush flushes both csv and compressor writer if
// initialized
func (c *csvExporter) Flush() error {
	c.csvWriter.Flush()
	if c.compressor != nil {
		return c.compressor.Flush()
	}
	return nil
}

// Bytes results in the slice of bytes with compressed content
func (c *csvExporter) Bytes() []byte {
	return c.buf.Bytes()
}

// Len returns length of the buffer with content
func (c *csvExporter) Len() int {
	return c.buf.Len()
}

func (c *csvExporter) FileName(spec distsqlpb.CSVWriterSpec, part string) string {
	pattern := exportFilePatternDefault
	if spec.NamePattern != "" {
		pattern = spec.NamePattern
	}

	fileName := strings.Replace(pattern, exportFilePatternPart, part, -1)
	// TODO: add suffix based on compressor type
	if c.compressor != nil {
		fileName += ".gz"
	}
	return fileName
}

// RunGzip used to generate compressed files
func RunGzip(
	bufBytes []byte, part string, sp distsqlpb.CSVWriterSpec,
) (p []byte, filename string, size int, err error) {
	buf := bytes.NewBuffer([]byte{})
	writer := gzip.NewWriter(buf)
	var exporter *csvExporter
	exporter = &csvExporter{
		compressor: writer,
		buf:        buf,
		csvWriter:  csv.NewWriter(writer),
	}
	_, err = exporter.WriteBytes(bufBytes)
	if err != nil {
		return nil, "", 0, err
	}
	filename = exporter.FileName(sp, part)
	err = exporter.Flush()
	if err != nil {
		return nil, "", 0, err
	}
	err = exporter.Close()
	if err != nil {
		return nil, "", 0, err
	}
	bufBytes = exporter.Bytes()
	size = exporter.Len()
	return bufBytes, filename, size, err
}

// GzipBytes used to generate compressed files in the backup
func GzipBytes(bufBytes []byte) (p []byte, size int64, err error) {
	buf := bytes.NewBuffer([]byte{})
	writer := gzip.NewWriter(buf)
	var exporter *csvExporter
	exporter = &csvExporter{
		compressor: writer,
		buf:        buf,
		csvWriter:  csv.NewWriter(writer),
	}
	_, err = exporter.WriteBytes(bufBytes)
	if err != nil {
		return nil, 0, err
	}
	err = exporter.Flush()
	if err != nil {
		return nil, 0, err
	}
	err = exporter.Close()
	if err != nil {
		return nil, 0, err
	}
	bufBytes = exporter.Bytes()
	size = int64(len(bufBytes))
	return bufBytes, size, err
}

type txtExporter struct {
	compressor *gzip.Writer
	buf        *bytes.Buffer
	txtWriter  *txt.Writer
}

// Write append record to txt file
//unused
//func (c *txtExporter) Write(record []string) error {
//	return c.txtWriter.Write(record)
//}

func (c *txtExporter) WriteBytes(p []byte) (int, error) {
	return c.txtWriter.WriteBytes(p)
}

// Close closes the compressor writer which
// appends archive footers
func (c *txtExporter) Close() error {
	if c.compressor != nil {
		return c.compressor.Close()
	}
	return nil
}

// Flush flushes both txt and compressor writer if
// initialized
func (c *txtExporter) Flush() error {
	c.txtWriter.Flush()
	if c.compressor != nil {
		return c.compressor.Flush()
	}
	return nil
}

// Bytes results in the slice of bytes with compressed content
func (c *txtExporter) Bytes() []byte {
	return c.buf.Bytes()
}

// Len returns length of the buffer with content
func (c *txtExporter) Len() int {
	return c.buf.Len()
}
func (c *txtExporter) FileName(spec distsqlpb.TXTWriterSpec, part string) string {
	pattern := exportFilePatternDefaultTXT
	if spec.NamePattern != "" {
		pattern = spec.NamePattern
	}

	fileName := strings.Replace(pattern, exportFilePatternPart, part, -1)
	// TODO: add suffix based on compressor type
	if c.compressor != nil {
		fileName += ".gz"
	}
	return fileName
}

// RunGzipTXT used to generate compressed files
func RunGzipTXT(
	bufBytes []byte, part string, sp distsqlpb.TXTWriterSpec,
) (p []byte, filename string, size int, err error) {
	buf := bytes.NewBuffer([]byte{})
	writer := gzip.NewWriter(buf)
	var exporter *txtExporter
	exporter = &txtExporter{
		compressor: writer,
		buf:        buf,
		txtWriter:  txt.NewWriter(writer),
	}
	_, err = exporter.WriteBytes(bufBytes)
	if err != nil {
		return nil, "", 0, err
	}
	filename = exporter.FileName(sp, part)
	err = exporter.Flush()
	if err != nil {
		return nil, "", 0, err
	}
	err = exporter.Close()
	if err != nil {
		return nil, "", 0, err
	}
	bufBytes = exporter.Bytes()
	size = exporter.Len()
	return bufBytes, filename, size, err
}
