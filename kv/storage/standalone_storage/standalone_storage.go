package standalone_storage

import (
	"errors"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	engines *engine_util.Engines
	config  *config.Config

	wg sync.WaitGroup
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	dbPath := conf.DBPath
	kvPath := filepath.Join(dbPath, "sa")
	os.MkdirAll(kvPath, os.ModePerm)
	kvDB := engine_util.CreateDB(kvPath, false)
	engines := engine_util.NewEngines(kvDB, nil, kvPath, "")

	return &StandAloneStorage{engines: engines, config: conf}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	// 调用DB的new事务方法。
	txn := s.engines.Kv.NewTransaction(false)
	if txn == nil {
		log.Fatal("can't open txn")
		return nil, errors.New("StandAloneStorage Reader new transaction error")
	}

	return NewStandAloneReader(txn), nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	// 这里需要将需要写入的内容都放到 WriteBatch 里，
	// 然后再使用 WriteBatch 的 WriteToDB 方法
	wb := &engine_util.WriteBatch{}
	wb.Reset()

	for _, m := range batch {
		switch m.Data.(type) {
		case storage.Put:
			put := m.Data.(storage.Put)
			wb.SetCF(put.Cf, put.Key, put.Value)
		case storage.Delete:
			delete := m.Data.(storage.Delete)
			wb.DeleteCF(delete.Cf, delete.Key)
		}
	}

	err := wb.WriteToDB(s.engines.Kv)
	if err != nil {
		return err
	}

	return nil
}
