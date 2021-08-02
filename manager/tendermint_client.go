/*
 * Copyright (C) 2020 The poly network Authors
 * This file is part of The poly network library.
 *
 * The  poly network  is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * The  poly network  is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License
 * along with The poly network .  If not, see <http://www.gnu.org/licenses/>.
 */

package manager

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/polynetwork/polygon-relayer/cosmos-sdk/codec"
	"github.com/polynetwork/polygon-relayer/db"
	"github.com/polynetwork/polygon-relayer/types"

	abcitypes "github.com/christianxiao/tendermint/abci/types"
	rpcclient "github.com/christianxiao/tendermint/rpc/client"
	tdmt_types "github.com/christianxiao/tendermint/types"

	hmTypes "github.com/polynetwork/polygon-relayer/heimdall/types"
	"github.com/polynetwork/polygon-relayer/log"

	mytypes "github.com/polynetwork/polygon-relayer/types"
)

type TendermintClient struct {
	RPCHttp *rpcclient.HTTP
	Codec   *codec.Codec

	db       *db.BoltDB
	exitChan chan int
}

var SpanPrefixKey = []byte{0x36} // prefix key to store span

// GetSpanKey appends prefix to start block
func GetSpanKey(id uint64) []byte {
	return append(SpanPrefixKey, []byte(strconv.FormatUint(id, 10))...)
}

func NewTendermintClient(addr string, db *db.BoltDB, cdc *codec.Codec, tclient *rpcclient.HTTP) (*TendermintClient, error) {
	c := tclient

	return &TendermintClient{
		RPCHttp:  c,
		Codec:    cdc,
		db:       db,
		exitChan: make(chan int),
	}, nil
}

type StartEnd struct {
	Start uint64
	End   uint64
}

type SpanStartEnd struct {
	ID       uint64
	StartEnd *StartEnd
}

func (this *TendermintClient) GetSpanIdByBor(bor uint64) (uint64, error) {
	all, err := this.db.GetAllUint64(db.BKTSpan)
	if err != nil {
		return 0, err
	}

	allStrt := make([]*SpanStartEnd, len(all))
	for _, v := range all {
		var startEnd = &StartEnd{}
		json.Unmarshal(v.V, startEnd)

		allStrt = append(allStrt, &SpanStartEnd{v.K, startEnd})

		if startEnd.Start <= (bor+1) && (bor+1) <= startEnd.End {
			return v.K, nil
		}
	}

	allStrtBytes, _ := json.MarshalIndent(allStrt, "", "    ")
	log.LogSpanL.Warnf("DB GetSpanIdByBor: span not found! bor height: %d, db data: %s", bor, string(allStrtBytes))

	return 0, fmt.Errorf("DB GetSpanIdByBor: span not found! bor height: %d, db data: %s, error: %w", bor, string(allStrtBytes), mytypes.ErrSpanNotFound)
}

func (this *TendermintClient) MonitorSpanLatestRoutine(seconds uint64) {
	log.LogSpanL.Infof("tendermint_client.MonitorSpanLatestRoutine - start, Duration %d", seconds)

	fetchBlockTicker := time.NewTicker(time.Duration(seconds) * time.Second)
	for {
		select {
		case <-fetchBlockTicker.C:
			h, err := this.GetLatestHeight()
			if err != nil {
				log.LogSpanL.Errorf("MonitorSpanLatestRoutine - GetLatestHeight error, err: %s", err.Error())
				continue
			}
			span, err := this.GetLatestSpan(h)
			if err != nil {
				log.LogSpanL.Errorf("MonitorSpanLatestRoutine - cannot get span from node height: %s err: %s", h, err.Error())
				continue
			}

			//check db
			val, err := this.db.GetUint64(db.BKTSpan, span.ID)
			if err != nil {
				log.LogSpanL.Errorf("MonitorSpanLatestRoutine - db.GetSpan error, spanId %s  error: %s", span.ID, err.Error())
			}
			varStrt := &StartEnd{}
			json.Unmarshal(val, varStrt)
			log.LogSpanL.Infof("MonitorSpanLatestRoutine - GetLatestHeight %d, lastest span: %d (%d-%d), db exist: %t",
				h, span.ID, span.StartBlock, span.EndBlock, len(val) != 0)

			var se = &StartEnd{
				Start: span.StartBlock,
				End:   span.EndBlock,
			}

			vjson, err := json.Marshal(se)
			if err != nil {
				log.LogSpanL.Errorf("MonitorSpanLatestRoutine - Marshal, err: %s", err.Error())
				continue
			}

			if bytes.Equal(vjson, val) {
				continue
			}

			err2 := this.db.PutUint64(db.BKTSpan, span.ID, vjson)
			if err2 != nil {
				log.LogSpanL.Errorf("MonitorSpanLatestRoutine - db.PutUint64 err: %v", err2)
				continue
			}
			log.LogSpanL.Infof("MonitorSpanLatestRoutine - db.PutUint64, span.id: %s, data: %s", span.ID, string(vjson))

		case <-this.exitChan:
			return
		}
	}
}

func (this *TendermintClient) MonitorSpanHisRoutine(start uint64) {
	log.LogSpanH.Infof("tendermint_client.MonitorSpanHisRoutine - start, start %d", start)

	for true {
		all, err := this.db.GetAllUint64(db.BKTSpan)
		if err != nil {
			log.LogSpanH.Errorf("MonitorSpanHisRoutine - error, err: %s", err.Error())
			time.Sleep(60 * time.Second)
			continue
		}
		log.LogSpanH.Debugf("MonitorSpanHisRoutine - db.GetAllSpan, data: %s", all)

		allmap := make(map[uint64][]byte)
		for _, v := range all {
			allmap[v.K] = v.V
		}

		if len(all) == 0 {
			continue
		}

		max := all[0].K
		for i := max; i >= start; i-- {
			// lastest pan may change, need to update everytime
			_, ok := allmap[i]
			if i == max || !ok {
				_, span, err := this.GetSpanRes(i, 0)
				if err != nil {
					log.LogSpanH.Errorf("MonitorSpanHisRoutine - GetSpanRes error, id %d, err: %s", i, err.Error())
					time.Sleep(10 * time.Second)
					continue
				}

				var se = &StartEnd{
					Start: span.StartBlock,
					End:   span.EndBlock,
				}

				vjson, err := json.Marshal(se)
				if err != nil {
					log.LogSpanH.Errorf("MonitorSpanHisRoutine - Marshal, err: %s", err.Error())
					continue
				}
				err2 := this.db.PutUint64(db.BKTSpan, span.ID, vjson)
				if err2 != nil {
					log.LogSpanH.Errorf("MonitorSpanHisRoutine - db.PutSpan err: %s", err2.Error())
					continue
				}
				log.LogSpanH.Infof("MonitorSpanHisRoutine - db.PutSpan, span.id: %d, data: %s", span.ID, string(vjson))
			}
		}

		time.Sleep(10 * time.Second)
	}
}

func (this *TendermintClient) GetLatestHeight() (int64, error) {
	statusRes, err := this.RPCHttp.Status()
	if err != nil {
		return 0, err
	}
	return statusRes.SyncInfo.LatestBlockHeight, nil
}

func (this *TendermintClient) GetLatestSpan(block int64) (*hmTypes.Span, error) {
	res, err := this.RPCHttp.ABCIQueryWithOptions(
		"custom/bor/latest-span",
		nil,
		rpcclient.ABCIQueryOptions{Prove: true, Height: block})
	if err != nil {
		log.Errorf("tendermint_client.GetSpanRes - failed, block %d, %v", block, err)
		return nil, err
	}

	var span = new(hmTypes.Span)
	err2 := json.Unmarshal(res.Response.Value, &span)
	if err2 != nil {
		log.Errorf("tendermint_client.GetLatestSpan - unmarshal failed, block %d, %v", block, err2)
		return nil, err2
	}

	return span, nil
}

// block: 0 = latest
func (this *TendermintClient) GetSpanRes(id uint64, heimHeight int64) (*abcitypes.ResponseQuery, *hmTypes.Span, error) {
	res, err := this.RPCHttp.ABCIQueryWithOptions(
		"/store/bor/key",
		GetSpanKey(id),
		rpcclient.ABCIQueryOptions{Prove: true, Height: heimHeight})
	if err != nil {
		return nil, nil, fmt.Errorf("tendermint_client.GetSpanRes - failed, spanID %d, heimHeight %d, %w", id, heimHeight, err)
	}

	if len(res.Response.Value) == 0 {
		// The spanID is too new in old heimHeight
		return nil, nil, fmt.Errorf("tendermint_client.GetSpanRes - failed, the spanID is too new in old heimHeight, res.Response.Value is nil, spanID %d, heimHeight %d, error: %w",
			id, heimHeight, types.ErrSpanNotFound)
	}

	if len(res.Response.Proof.GetOps()) == 0 {
		// The spanID is too new in old heimHeight
		return nil, nil, fmt.Errorf("tendermint_client.GetSpanRes - failed, the spanID is too new in old heimHeight, res.Response.Proof is nil, spanID %d, heimHeight %d, error: %w",
			id, heimHeight, types.ErrSpanNotFound)
	}

	if len(res.Response.Key) == 0 {
		// The spanID is too new in old heimHeight
		return nil, nil, fmt.Errorf("tendermint_client.GetSpanRes - failed, the spanID is too new in old heimHeight, res.Response.Key is nil, spanID %d, heimHeight %d, error: %w",
			id, heimHeight, types.ErrSpanNotFound)
	}

	var span = new(hmTypes.Span)
	err2 := this.Codec.UnmarshalBinaryBare(res.Response.Value[:], span)
	if err2 != nil {
		return nil, nil, fmt.Errorf("tendermint_client.GetSpan - unmarshal failed, id %d, heimHeight %d, %w", id, heimHeight, err)
	}

	return &res.Response, span, nil
}

func (this *TendermintClient) GetCosmosHdr(h int64) (*types.CosmosHeader, error) {
	log.Debugf("bor time analyse - this.RPCHttp.Commit start, bor height: %d", h)
	rc, err := this.RPCHttp.Commit(&h)
	log.Debugf("bor time analyse - this.RPCHttp.Commit   end, bor height: %d", h)
	if err != nil {
		return nil, fmt.Errorf("tendermint_client.GetCosmosHdr - commit error, to get Commit of height %d: %v", h, err)
	}

	log.Debugf("bor time analyse - this.getValidators start, bor height: %d", h)
	vSet, err := this.getValidators(h)
	log.Debugf("bor time analyse - this.getValidators   end, bor height: %d", h)
	
	if err != nil {
		return nil, fmt.Errorf("failed to get Validators of height %d: %v", h, err)
	}
	return &types.CosmosHeader{
		Header:  *rc.Header,
		Commit:  rc.Commit,
		Valsets: vSet,
	}, nil
}

// TODO: this only return first 100 items
func (this *TendermintClient) getValidators(h int64) ([]*tdmt_types.Validator, error) {
	vSet := make([]*tdmt_types.Validator, 0)

	res, err := this.RPCHttp.Validators(&h)
	if err != nil {
		if strings.Contains(err.Error(), "page should be within") {
			return vSet, nil
		}
		return nil, err
	}
	// In case tendermint don't give relayer the right error
	if len(res.Validators) == 0 {
		return vSet, nil
	}
	vSet = append(vSet, res.Validators...)

	return vSet, nil
}
