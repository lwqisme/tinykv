package server

import (
	"context"

	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	res := &kvrpcpb.RawGetResponse{}

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		res.Error = err.Error()
		return res, err
	}

	value, err := reader.GetCF(req.Cf, req.Key)
	if err != nil {
		res.Error = err.Error()
		return res, err
	}
	if value != nil {
		res.Value = value
	} else {
		res.NotFound = true
	}

	return res, err
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	res := &kvrpcpb.RawPutResponse{}

	modify := storage.Modify{}
	modify.Data = storage.Put{
		Key:   req.Key,
		Value: req.Value,
		Cf:    req.Cf,
	}

	err := server.storage.Write(req.Context, []storage.Modify{modify})
	if err != nil {
		res.Error = err.Error()
		return res, err
	}

	return res, nil
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be deleted
	res := &kvrpcpb.RawDeleteResponse{}

	modify := storage.Modify{}
	modify.Data = storage.Delete{
		Key: req.Key,
		Cf:  req.Cf,
	}

	err := server.storage.Write(req.Context, []storage.Modify{modify})
	if err != nil {
		res.Error = err.Error()
		return res, err
	}
	return res, nil
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF
	res := &kvrpcpb.RawScanResponse{}

	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		res.Error = err.Error()
		return res, err
	}

	iter := reader.IterCF(req.Cf)
	// Seek 操作不包括当前key
	iter.Seek(req.StartKey)

	for i := 0; iter.Valid() && i < int(req.Limit); i++ {
		k := iter.Item().Key()
		v, err := iter.Item().Value()
		if err != nil {
			res.Kvs = append(res.Kvs, &kvrpcpb.KvPair{
				Error: &kvrpcpb.KeyError{},
				Key:   k,
			})
		} else {
			res.Kvs = append(res.Kvs, &kvrpcpb.KvPair{
				Key:   k,
				Value: v,
			})
		}
		iter.Next()
	}

	return res, nil
}
