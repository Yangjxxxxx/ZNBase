package rowexec

import (
	"runtime"

	"github.com/znbasedb/znbase/pkg/settings"
	"github.com/znbasedb/znbase/pkg/sql/distsqlrun/runbase"
	"github.com/znbasedb/znbase/pkg/sql/sqlbase"
)

// SorterOpt for sort opt switch
var SorterOpt *settings.BoolSetting

// SorterParallelNums for sort opt parallel num
var SorterParallelNums *settings.IntSetting

func init() {
	SorterOpt = settings.RegisterBoolSetting(
		"sql.opt.operator.sorter",
		"for sort opt",
		false,
	)
	SorterParallelNums = settings.RegisterIntSetting(
		"sql.opt.operator.sorter.parallelnum",
		"for sort nums",
		2,
	)
}

type tempResult struct {
	row sqlbase.EncDatumRow
	idx int
}

func (s *sortAllProcessor) PopResultRowToChannel(reschan chan *runbase.ChunkBuf) {
	runtime.LockOSThread()
	defer func() {
		runtime.UnlockOSThread()
	}()
	var finished, ok bool
	var err error
	var rowBuf = new(runbase.ChunkBuf)
	rowBuf.Buffer = make([]*runbase.ChunkRow, RowBufferSize)
	for j := 0; j < RowBufferSize; j++ {
		rowBuf.Buffer[j] = new(runbase.ChunkRow)
		rowBuf.Buffer[j].Row = make(sqlbase.EncDatumRow, s.RowLen)
	}
	rowBuf.MaxIdx = -1
	for {
		finished = false
		for i := range s.optI {
			if ok, err = s.optI[i].Valid(); err != nil {
				close(reschan)
			}
			s.optValid[i] = ok
			finished = finished || ok
		}
		if !finished {
			if rowBuf.MaxIdx != -1 {
				reschan <- rowBuf
			}
			close(reschan)
			break
		}
		if s.tempResult.idx != -1 {
			if s.optValid[s.tempResult.idx] {
				if s.UseTempFile && s.frc[s.tempResult.idx].Spilled() {
					row, err := s.optI[s.tempResult.idx].Row()
					if err != nil {
						s.MoveToDraining(err)
						close(reschan)
						break
					}
					s.resRows[s.tempResult.idx], err = s.frc[s.tempResult.idx].GetRow(row)
					if err != nil {
						s.MoveToDraining(err)
						close(reschan)
						break
					}
				} else {
					s.resRows[s.tempResult.idx], err = s.optI[s.tempResult.idx].Row()
					if err != nil {
						s.MoveToDraining(err)
						close(reschan)
						break
					}
				}
			} else {
				s.resRows[s.tempResult.idx] = nil
			}
		} else {
			for i := range s.optI {
				if s.optValid[i] {
					if s.UseTempFile && s.frc[i].Spilled() {
						row, err := s.optI[i].Row()
						if err != nil {
							s.MoveToDraining(err)
							close(reschan)
							break
						}
						s.resRows[i], err = s.frc[i].GetRow(row)
						if err != nil {
							s.MoveToDraining(err)
							close(reschan)
							break
						}
					} else {
						s.resRows[i], err = s.optI[i].Row()
						if err != nil {
							s.MoveToDraining(err)
							close(reschan)
							break
						}
					}
				}
			}
		}
		s.tempResult = s.PopResultRowAndIdx(s.resRows)
		s.optI[s.tempResult.idx].Next()

		if runbase.PushToBuf(rowBuf, s.tempResult.row, nil, RowBufferSize) {
			reschan <- rowBuf
			select {
			case rowBuf = <-s.recycleRows:
			default:
				rowBuf = new(runbase.ChunkBuf)
				rowBuf.Buffer = make([]*runbase.ChunkRow, RowBufferSize)
				for j := 0; j < RowBufferSize; j++ {
					rowBuf.Buffer[j] = new(runbase.ChunkRow)
					rowBuf.Buffer[j].Row = make(sqlbase.EncDatumRow, s.RowLen)
				}
				rowBuf.MaxIdx = -1
			}
		}
	}
}
