package raftstore

import (
	"fmt"
	"time"

	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/raft"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
)

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx *GlobalContext
}

func newPeerMsgHandler(peer *peer, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer: peer,
		ctx:  ctx,
	}
}

func (d *peerMsgHandler) HandleRaftReady() {
	if d.stopped {
		return
	}
	// Your Code Here (2B).

	if d.peer.RaftGroup.HasReady() {
		// 保存 ready 中的内容
		// 保存 ready 之后，在后续的 Advance 中更新对应 stabled 以及 applied

		ready := d.RaftGroup.Ready()
		_, err := d.peerStorage.SaveReadyState(&ready)
		if err != nil {
			return
		}
		// 发送 msgs
		d.Send(d.ctx.trans, ready.Messages)

		// 调用 ApplyCommitedEntries 之前，这些 CommittedEntries 已经被上面的
		// saveReadyState 给持久化了，现在可以放心的将它们 apply。
		err = d.ApplyCommitedEntries(ready.CommittedEntries)
		if err != nil {
			return
		}

		// 设置 region
		// GlobalContext.storeMeta
		wb := engine_util.WriteBatch{}
		region := d.Region()
		meta.WriteRegionState(&wb, region, rspb.PeerState_Normal)
		err = wb.WriteToDB(d.peerStorage.Engines.Kv)
		if err != nil {
			panic("write batch HandleRaftReady error")
			return
		}

		// 更新 stabled 和 applied
		d.RaftGroup.Advance(ready)
	}
}

// ApplyCommitedEntries 写数据库和读数据库最终的处理都在这里
func (d *peerMsgHandler) ApplyCommitedEntries(committedEntrties []eraftpb.Entry) error {
	raftLogEntriesWb := engine_util.WriteBatch{}
	// applyState := new(rspb.RaftApplyState)

	if !d.IsLeader() {
		d.proposals = make(map[uint64]*proposal)
	}

	// TODO：这里的 cbs 最好改成存储 proposals，因为我们处理完后需要从proposals中删除这部分的proposal
	cbs := make(map[*message.Callback]bool, 0)
	for _, ent := range committedEntrties {
		// TruncatedState 用于 2C ，所以暂时可以不用管。

		// 真正的运行 entries 里的命令！
		// now execute the command in the committedEntries.
		req := raft_cmdpb.Request{}
		err := req.Unmarshal(ent.Data)
		if err != nil {
			// return err
			continue
		}
		// ent.Data 为0是 becomeLeader 或者更新 commit，并不算是一个错误
		// 这种情况也不需要获取 proposal 去完成 callback
		if len(ent.Data) == 0 {
			continue
		}

		// 直接获取 proposals ，就不需要处理冲突了。
		proposal := d.proposals[ent.Index]

		if proposal == nil {
			// 非 leader 没有 proposal
			if d.IsLeader() && req.CmdType == raft_cmdpb.CmdType_Put {
				log.Infof("qq: isleader, id: %v, get nil proposal ,want apply put key:%s, put value:%s", d.PeerId(), string(req.Put.Key), string(req.Put.Value))
			}
		} else {
			cbs[proposal.cb] = true
		}

		// 每一个循环只用一次，避免循环中同时出现 put 和 snap 导致没有及时写入数据库。
		wb := engine_util.WriteBatch{}
		switch req.CmdType {
		case raft_cmdpb.CmdType_Get:
			log.Infof("qq: id:%v apply get index:%v", d.PeerId(), ent.Index)

			get := req.Get
			// TODO: CF 全部统统改为写入 KV 数据库
			val, err := engine_util.GetCF(d.peerStorage.Engines.Kv, get.Cf, get.Key)
			if proposal != nil {
				if err != nil {
					proposal.cb.Done(ErrResp(err))
					// return err
					continue
				}
				resp := newCmdResp()
				resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
					CmdType: raft_cmdpb.CmdType_Get,
					Get: &raft_cmdpb.GetResponse{
						Value: val,
					},
				})
				proposal.cb.Resp = resp
			}
			if err != nil {
				// return err
				continue
			}
		case raft_cmdpb.CmdType_Put:
			put := req.Put
			wb.SetCF(put.Cf, put.Key, put.Value)
			if string(put.Key) == "k1" {
				log.Infof("qq: id: %v, put: %v", d.PeerId(), put)
			}
			log.Infof("qq: id:%v apply put value:%v, index:%v", d.PeerId(), string(put.Value), ent.Index)

			err := wb.WriteToDB(d.peerStorage.Engines.Kv)
			if err != nil {
				panic("write batch entries data to db error!")
			}
			if proposal != nil {
				resp := newCmdResp()
				resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
					CmdType: raft_cmdpb.CmdType_Put,
					Put:     &raft_cmdpb.PutResponse{},
				})
				proposal.cb.Resp = resp
			}
		case raft_cmdpb.CmdType_Delete:
			del := req.Delete
			wb.DeleteCF(del.Cf, del.Key)
			log.Infof("qq: id:%v apply delete value:%v, index:%v", d.PeerId(), string(del.Key), ent.Index)

			err := wb.WriteToDB(d.peerStorage.Engines.Kv)
			if err != nil {
				panic("write batch entries data to db error!")
			}
			if proposal != nil {
				resp := newCmdResp()
				resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
					CmdType: raft_cmdpb.CmdType_Delete,
					Delete:  &raft_cmdpb.DeleteResponse{},
				})
				proposal.cb.Resp = resp
			}
		case raft_cmdpb.CmdType_Snap:
			// 即便是 Snap 我们也添加到了 entries 中，到了apply时才对其进行处理并且返回
			log.Infof("qq: id:%v apply snap index:%v", d.PeerId(), ent.Index)

			if proposal != nil {
				resp := newCmdResp()
				resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
					CmdType: raft_cmdpb.CmdType_Snap,
					Snap:    &raft_cmdpb.SnapResponse{Region: d.Region()},
				})
				proposal.cb.Resp = resp
				proposal.cb.Txn = d.ctx.engine.Kv.NewTransaction(false)
				log.Infof("qq: id:%v leader set snap index:%v", d.PeerId(), ent.Index)
			}
		default:
			errMsg := "error CmdType while executing committed entries"
			log.Errorf(errMsg)
			// return fmt.Errorf(errMsg)
			continue
		}
		// TODO: 这里的 AppliedIndex 更新应该是需要放到后面去一点，否则更新失败了的时候index就错了。
		d.peerStorage.applyState.AppliedIndex = ent.Index
		raftLogEntriesWb.SetMeta(meta.RaftLogKey(d.regionId, ent.Index), &ent)
	}

	// 如果这个有需要也可以放到 每一次循环当中去写数据库。
	err := raftLogEntriesWb.WriteToDB(d.peerStorage.Engines.Raft)
	if err != nil {
		panic("write batch raft log entries to db error!")
	}

	applyStateWb := engine_util.WriteBatch{}
	// TODO：应该不需要每一条都写入 applystate 吧？我只管写，好像现在也用不上在哪里读。
	err = applyStateWb.SetMeta(meta.ApplyStateKey(d.regionId), d.peerStorage.applyState)
	if err != nil {
		// panic("write batch write apply state error! error:")
		return err
	}
	err = applyStateWb.WriteToDB(d.peerStorage.Engines.Kv)
	if err != nil {
		panic("wirte batch apply state to db error!")
	}

	// 目前认为，拥有 callback 的 entries 都是外部调用，经过MustPut进入系统。而如此进入系统的都是
	// 一个 callback 对应一条 entry 的。所以可以直接对 callback 进行 Done。
	for cb, _ := range cbs {
		BindRespTerm(cb.Resp, d.Term())
		cb.Done(cb.Resp)
	}

	return nil
}

func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
			// log.Errorf("qq: error msg: %v", raftMsg)
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		log.Debugf("qq: id:%v handle MsgTypeRaftCmd, cmd: %v", d.storeID(), raftCMD)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.Infof("%s on split with %v", d.Tag, split.SplitKey)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	}
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}

// proposeRaftCommand 不能直接操作数据库，其实是调用 Rawnode 中的 propose ，提出 raft 请求
// 接收到了 request ，现在已经开始进行处理，只有 leader 才配处理 cmd ，所以此处必须是 leader
func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	// if !d.IsLeader() {
	// 	log.Warnf("qq: id:%v not header proposing raft command, requests:%v", d.PeerId(), msg.Requests)
	// 	cb.Done(ErrResp(&util.ErrNotLeader{}))
	// 	return
	// }

	// ErrStaleCommand is set here
	// 这里面同时也检查了是否leader，不用在上面自己检查一遍了
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	// TODO：所以问题可能出在这里，就是每一次传入的时候我都直接设置了Respposes。
	// 导致传出去的时候没有触发错误
	// cb.Resp = new(raft_cmdpb.RaftCmdResponse)
	// cb.Resp.Header = new(raft_cmdpb.RaftResponseHeader)
	// cb.Resp.Responses = make([]*raft_cmdpb.Response, 0)
	// cb.Resp.AdminResponse = nil // 这个目前我不清楚是做什么用的。

	// hasWrite := false
	// Your Code Here (2B).
	// TODO：难道只有 leader 才配处理请求？
	for _, req := range msg.Requests {
		switch req.CmdType {
		case raft_cmdpb.CmdType_Get:
			// TODO：GetCF 方法是在内部新建 txn ，如果错误不知道能不能正确释放，
			// 如果出错需要考虑这里换为手动创建 txn。

			// 通过 CF 获取的这一个逻辑实际上是独立的，有对 CF 进行操作的只有 apply 的时候(在上面的ApplyCommitted)以及这个 Get 的时候
			// val, err := engine_util.GetCF(d.peerStorage.Engines.Raft, req.Get.Cf, req.Get.Key)
			// if err != nil {
			// 	cb.Done(ErrResp(err))
			// 	return
			// }
			// resp := newCmdResp()
			// resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
			// 	Get: &raft_cmdpb.GetResponse{
			// 		Value: val,
			// 	},
			// })
			// cb.Resp = resp
			if d.RaftGroup.Raft.State != raft.StateLeader {
				cb.Done(ErrResp(&util.ErrNotLeader{}))
				return
			}
			data, err := req.Marshal()
			if err != nil {
				cb.Done(ErrResp(err))
				return
			}

			d.appendOneProposal(cb)
			d.RaftGroup.Propose(data)
		case raft_cmdpb.CmdType_Put:
			// hasWrite = true
			if d.RaftGroup.Raft.State != raft.StateLeader {
				cb.Done(ErrResp(&util.ErrNotLeader{}))
				return
			}

			data, err := req.Marshal()
			if err != nil {
				cb.Done(ErrResp(err))
				return
			}
			// TODO：在append之前需要检查是否有比 要加进去的index大的，或者term不一致的，
			// TODO：此时需要将冲突部分cb结束并且清理，保持proposals的有序性！！
			// d.proposals = append(d.proposals, &proposal{
			// 	index: d.nextProposalIndex(), // 这个地方似乎并不能保证线程安全。
			// 	term:  d.RaftGroup.Raft.Term,
			// 	cb:    cb,
			// })
			// ------------------------
			d.appendOneProposal(cb)
			// ------------------------
			d.RaftGroup.Propose(data)
		case raft_cmdpb.CmdType_Delete:
			// hasWrite = true
			if d.RaftGroup.Raft.State != raft.StateLeader {
				cb.Done(ErrResp(&util.ErrNotLeader{}))
				return
			}

			data, err := req.Marshal()
			if err != nil {
				cb.Done(ErrResp(err))
				return
			}
			// d.proposals = append(d.proposals, &proposal{
			// 	index: d.nextProposalIndex(), // 这个地方似乎并不能保证线程安全。
			// 	term:  d.RaftGroup.Raft.Term,
			// 	cb:    cb,
			// })
			// ------------------------
			d.appendOneProposal(cb)
			// ------------------------
			d.RaftGroup.Propose(data)
		case raft_cmdpb.CmdType_Snap:
			// 还是需要到apply那里把这个数据进行持久化之后再返回transaction

			// cb.Txn = d.ctx.engine.Raft.NewTransaction(false)
			// resp := newCmdResp()
			// resp.Responses = append(resp.Responses, &raft_cmdpb.Response{
			// 	CmdType: raft_cmdpb.CmdType_Snap,
			// 	Snap: &raft_cmdpb.SnapResponse{
			// 		Region: d.Region(),
			// 	},
			// })
			// cb.Resp = resp
			if d.RaftGroup.Raft.State != raft.StateLeader {
				cb.Done(ErrResp(&util.ErrNotLeader{}))
				return
			}
			data, err := req.Marshal()
			if err != nil {
				cb.Done(ErrResp(err))
				return
			}

			d.appendOneProposal(cb)
			d.RaftGroup.Propose(data)
		default:
		}
	}
	// if !hasWrite {
	// 	cb.Done(cb.Resp)
	// }
}

// appendOneProposal 只有 leader 会调用到此方法
func (d *peerMsgHandler) appendOneProposal(cb *message.Callback) {
	newIndex := d.nextProposalIndex()
	newTerm := d.RaftGroup.Raft.Term

	oldP := d.proposals[newIndex]
	if oldP != nil {
		// 当发现之前已经保存过相同 index 的 proposal 的时候
		if oldP.term >= newTerm {
			cb.Done(ErrRespStaleCommand(oldP.term))
			log.Errorf("qq: id:%v, oldP:%v win, newIndex:%v, newTerm:%v", d.PeerId(), oldP, newIndex, newTerm)
			return
		} else {
			oldP.cb.Done(ErrRespStaleCommand(newTerm))
			log.Errorf("qq: id:%v, oldP %v lose, newIndex:%v, newTerm:%v", d.PeerId(), oldP, newIndex, newTerm)
		}
	}
	d.proposals[newIndex] = &proposal{
		index: newIndex, // 这个地方似乎并不能保证线程安全。
		term:  newTerm,
		cb:    cb,
	}
}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	log.Debugf("%s handle raft message %s from %d to %d",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

// / Checks if the message is sent to the correct peer.
// /
// / Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    toPeer,
		ToPeer:      fromPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *peerMsgHandler) destroyPeer() {
	log.Infof("%s starts destroy", d.Tag)
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		panic(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}
	if !d.IsLeader() {
		return
	}
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}
