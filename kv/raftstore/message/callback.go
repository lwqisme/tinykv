package message

import (
	"time"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
)

type Callback struct {
	Resp *raft_cmdpb.RaftCmdResponse
	Txn  *badger.Txn // used for GetSnap
	done chan struct{}
	// TODO request批量这个事情，有可能一个callback对应好几个request/entities，其实可以在callback当中做一个计数，处理完一个，就append到resp当中一个，达到计数之后才执行done，而不是每一个request结束都进行done。
}

func (cb *Callback) Done(resp *raft_cmdpb.RaftCmdResponse) {
	if cb == nil {
		return
	}
	if resp != nil {
		cb.Resp = resp
	}
	cb.done <- struct{}{}
}

func (cb *Callback) WaitResp() *raft_cmdpb.RaftCmdResponse {
	select {
	case <-cb.done:
		return cb.Resp
	}
}

func (cb *Callback) WaitRespWithTimeout(timeout time.Duration,req *raft_cmdpb.RaftCmdRequest) *raft_cmdpb.RaftCmdResponse {
	select {
	case <-cb.done:
		return cb.Resp
	case <-time.After(timeout):
		log.Infof("qq: timeout:%v, req:%v, resp:%v", timeout, req, cb.Resp)
		return cb.Resp
	}
}

func NewCallback() *Callback {
	done := make(chan struct{}, 1)
	cb := &Callback{done: done}
	return cb
}
