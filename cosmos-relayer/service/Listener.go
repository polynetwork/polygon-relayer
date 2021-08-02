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

package service

import (
	"bytes"
	"fmt"
	"strings"
	"time"

	"github.com/polynetwork/poly/common"
	mhcomm "github.com/polynetwork/poly/native/service/header_sync/common"
	"github.com/polynetwork/poly/native/service/utils"
	polycosmos "github.com/polynetwork/polygon-relayer/poly/native/header_sync/cosmos"
	cosmos "github.com/polynetwork/polygon-relayer/types"

	tdmt_types "github.com/christianxiao/tendermint/types"

	"github.com/polynetwork/polygon-relayer/cosmos-relayer/context"
	"github.com/polynetwork/polygon-relayer/log"
)

var (
	ctx = context.RCtx
)

// Start listen cosmos and Poly
func StartListen() {
	// go PolyListen()
	go CosmosListen()
}

// Cosmos listen service implementation. Check the blocks of COSMOS from height
// `left` to height `right`, commit the cross-chain txs and headers to prove txs
// to chain Poly. It execute once per `ctx.Conf.CosmosListenInterval` sec. And update
// height `left` `right` after execution for next round. This function will run
// as a go-routine.
func CosmosListen() {
	log.LogTender.Infof("Listerner.CosmosListen - start")

	left, tick, err := beforeCosmosListen()
	if err != nil {
		log.LogTender.Fatalf("[ListenCosmos] failed to get start height of Cosmos: %v", err)
		panic(err)
	}
	log.LogTender.Infof("[ListenCosmos] start listen Cosmos (start_height: %d, listen_interval: %d)", left+1,
		ctx.Conf.CosmosListenInterval)

	lastRight := left
	for {
		select {
		case <-tick.C:
			status, err := ctx.CMRpcCli.Status()
		
			switch {
			case err != nil:
				log.LogTender.Errorf("[ListenCosmos] failed to get height of COSMOS, retry after %d sec: %v",
					ctx.Conf.CosmosListenInterval, err)
				continue
			case status.SyncInfo.LatestBlockHeight-1 <= lastRight:
				continue
			}

			log.LogTender.Infof("[ListenCosmos] status: left: %d, status: %d, diff: %d", left, status.SyncInfo.LatestBlockHeight, status.SyncInfo.LatestBlockHeight-left)
			right := status.SyncInfo.LatestBlockHeight - 1
			log.LogTender.Infof("[ListenCosmos] CosmosListen left: %d, right: %d, diff: %d", left, right, right-left)

			//var hdr *cosmos.CosmosHeader
/* 			hdr, err := getCosmosHdr(right)
			if err != nil {
				log.LogTender.Errorf("[ListenCosmos] failed to get %d header to get proof, retry after %d sec: %v",
					right, ctx.Conf.CosmosListenInterval, err)
				continue
			}
			log.LogTender.Infof("[ListenCosmos] getCosmosHdr height: %d, appHash: %s", right, hdr.Header.AppHash)

			if !bytes.Equal(hdr.Header.ValidatorsHash, hdr.Header.NextValidatorsHash) {
				log.LogTender.Infof("[ListenCosmos] header at %d is epoch switching point, so continue loop", hdr.Header.Height)
				lastRight = right
				continue
			} */

			// let first element of infoArr be the info for epoch switching headers.
			infoArr := &context.CosmosInfo{
				Type: context.TyHeader,
				Hdrs: make([]*cosmos.CosmosHeader, 0),
			}

			for h := left + 1; h <= right; h++ {
				infoArrTemp, err := checkCosmosHeight(h, infoArr)
				if err != nil {
					log.LogTender.Errorf("[ListenCosmos] checkCosmosHeight error: height: %d right: %d error: %w", h, right, err)
					// If error happen, we should check this height again.
					h--
					if strings.Contains(err.Error(), context.RightHeightUpdate) {
						// Can't get proof from height `right-1`, update right to the latest.
						log.LogTender.Errorf("[ListenCosmos] checkCosmosHeight height: %s right: %s error: %w", h, right, err)
						continue
					}
					// some error happen, could be some network error or COSMOS full node error.
					log.LogTender.Errorf("[ListenCosmos] failed to fetch info from COSMOS, retry after 3 sec: %s", err)
					context.SleepSecs(3)
					continue
				}
				infoArr = infoArrTemp

				log.LogTender.Infof("[ListenCosmos] left %d, right %d, h %d, infoArr.Hdrs: %d batch: %d",
					left, right, h,
					len(infoArr.Hdrs), ctx.Conf.HeadersPerBatch)
				if len(infoArr.Hdrs) >= ctx.Conf.HeadersPerBatch || h == right {
					log.LogTender.Infof("[ListenCosmos] ctx.ToPoly - left %d, right %d, h %d, infoArr.Hdrs: %d batch: %d data: %w",
						left, right, h,
						len(infoArr.Hdrs), ctx.Conf.HeadersPerBatch, infoArr)
					ctx.ToPoly <- infoArr

					ctx.ToPoly <- &context.CosmosInfo{
						Type:   context.TyUpdateHeight,
						Height: h,
					}

					infoArr = &context.CosmosInfo{
						Type: context.TyHeader,
						Hdrs: make([]*cosmos.CosmosHeader, 0),
					}
				}
			}

			lastRight = right
			left = right

			/* for i, v := range infoArr {
				if i == 0 && len(v.Hdrs) == 0 {
					continue
				}
				ctx.ToPoly <- v
			}

			ctx.ToPoly <- &context.CosmosInfo{
				Type:   context.TyUpdateHeight,
				Height: right,
			}
			lastRight = right
			left = right */
		}
	}
}

func GetBestCosmosHeightForBor() (int64, error) {	
	currHeight, err := GetCosmosHeightFromPoly()
	if err != nil {
		return 0, err
	}

	log.LogTender.Infof("GetBestCosmosHeightForBor, ( cosmos height on Poly: %d )", currHeight)

	if dbh := ctx.Db.GetCosmosHeight(); dbh > currHeight {
		log.LogTender.Infof("GetBestCosmosHeightForBor, ( cosmos height in DB: %d )", dbh)
		currHeight = dbh
	}
	
	return currHeight, nil
}

// Prepare start height and ticker when init service
func beforeCosmosListen() (int64, *time.Ticker, error) {
	currHeight, err := GetCosmosHeightFromPoly()
	if err != nil {
		return 0, nil, err
	}

	log.LogTender.Infof("beforeCosmosListen, ( cosmos height on Poly: %d )", currHeight)

	if dbh := ctx.Db.GetCosmosHeight(); dbh > currHeight {
		log.LogTender.Infof("beforeCosmosListen, ( cosmos height in DB: %d )", dbh)
		currHeight = dbh
	}
	if ctx.Conf.CosmosStartHeight != 0 {
		currHeight = ctx.Conf.CosmosStartHeight
	}
	return currHeight, time.NewTicker(time.Duration(ctx.Conf.CosmosListenInterval) * time.Second), nil
}

func GetCosmosHeightFromPoly() (int64, error) {
	val, err := ctx.Poly.GetStorage(utils.HeaderSyncContractAddress.ToHexString(),
		append([]byte(mhcomm.EPOCH_SWITCH), utils.GetUint64Bytes(ctx.Conf.SideChainId)...))
	if err != nil {
		return 0, err
	}
	info := &polycosmos.CosmosEpochSwitchInfo{}
	if err = info.Deserialization(common.NewZeroCopySource(val)); err != nil {
		return 0, err
	}
	currHeight := info.Height
	/* if currHeight > 1 {
		currHeight--
	} */

	return currHeight, nil
}

// Fetch header at h and check tx at h-1.
//
// Put header to `hdrArr` and txs to `txArr`. Get proof from height `heightToGetProof`.
// `headersToRelay` record all hdrs need to relay. When need to update new height to
// get proof, relayer update `rightPtr` and return.
func checkCosmosHeight(h int64, infoArr *context.CosmosInfo) (*context.CosmosInfo, error) {
	rc, err := ctx.CMRpcCli.Commit(&h)
	if err != nil {
		return infoArr, err
	}
	log.LogTender.Infof("listener.checkCosmosHeight, height: %d, NextValidatorsHash: %t",
		h, !bytes.Equal(rc.Header.ValidatorsHash, rc.Header.NextValidatorsHash))

	if !bytes.Equal(rc.Header.ValidatorsHash, rc.Header.NextValidatorsHash) {
		vSet, err := getValidators(h)
		if err != nil {
			return infoArr, err
		}
		hdr := &cosmos.CosmosHeader{
			Header:  *rc.Header,
			Commit:  rc.Commit,
			Valsets: vSet,
		}
		val, err := ctx.Poly.GetStorage(utils.CrossChainManagerContractAddress.ToHexString(),
			append(append([]byte(mhcomm.EPOCH_SWITCH), utils.GetUint64Bytes(ctx.Conf.SideChainId)...),
				utils.GetUint64Bytes(uint64(h))...))
		if err != nil {
			return infoArr, err
		}
		// check if this header is not committed on Poly
		if len(val) == 0 {
			infoArr.Hdrs = append(infoArr.Hdrs, hdr)
		}
	}

	return infoArr, nil
}

func getValidators(h int64) ([]*tdmt_types.Validator, error) {

	vSet := make([]*tdmt_types.Validator, 0)

	// TODO: this changed!, don't know how to get full validators, only return first 100
	res, err := ctx.CMRpcCli.Validators(&h)
	if err != nil {
		if strings.Contains(err.Error(), "page should be within") {
			return vSet, nil
		}
		return nil, err
	}
	log.LogTender.Infof("cosmos getValidators - height: %d, validators: %d ", h, len(res.Validators))
	// In case tendermint don't give relayer the right error
	if len(res.Validators) == 0 {
		return vSet, nil
	}
	vSet = append(vSet, res.Validators...)

	return vSet, nil
}

func getCosmosHdr(h int64) (*cosmos.CosmosHeader, error) {
	rc, err := ctx.CMRpcCli.Commit(&h)
	if err != nil {
		return nil, fmt.Errorf("failed to get Commit of height %d: %v", h, err)
	}
	vSet, err := getValidators(h)
	if err != nil {
		return nil, fmt.Errorf("failed to get Validators of height %d: %v", h, err)
	}
	return &cosmos.CosmosHeader{
		Header:  *rc.Header,
		Commit:  rc.Commit,
		Valsets: vSet,
	}, nil
}
