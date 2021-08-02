/*
* Copyright (C) 2020 The poly network Authors
* This file is part of The poly network library.
*
* The poly network is free software: you can redistribute it and/or modify
* it under the terms of the GNU Lesser General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* The poly network is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
* GNU Lesser General Public License for more details.
* You should have received a copy of the GNU Lesser General Public License
* along with The poly network . If not, see <http://www.gnu.org/licenses/>.
 */
package manager

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ontio/ontology/smartcontract/service/native/cross_chain/cross_chain_manager"
	"github.com/polynetwork/eth-contracts/go_abi/eccm_abi"
	common2 "github.com/polynetwork/poly/native/service/cross_chain_manager/common"
	"github.com/polynetwork/polygon-relayer/config"
	"github.com/polynetwork/polygon-relayer/cosmos-relayer/service"
	"github.com/polynetwork/polygon-relayer/cosmos-sdk/codec"
	"github.com/polynetwork/polygon-relayer/db"
	"github.com/polynetwork/polygon-relayer/types"
	mytypes "github.com/polynetwork/polygon-relayer/types"

	"github.com/christianxiao/tendermint/crypto/merkle"
	rpcclient "github.com/christianxiao/tendermint/rpc/client"

	"context"

	"github.com/ethereum/go-ethereum/common/hexutil"
	sdk "github.com/polynetwork/poly-go-sdk"
	"github.com/polynetwork/poly/common"
	"github.com/polynetwork/poly/native/service/cross_chain_manager/eth"
	scom "github.com/polynetwork/poly/native/service/header_sync/common"
	autils "github.com/polynetwork/poly/native/service/utils"
	"github.com/polynetwork/polygon-relayer/log"
	sdkp "github.com/polynetwork/polygon-relayer/poly_go_sdk"
	"github.com/polynetwork/polygon-relayer/tools"

	ethtypes "github.com/ethereum/go-ethereum/core/types"
)

var Sprint uint64 = 64

var StartFirst = true
var StartSpan uint64 = 0

var SpanIdCacheSynced uint64 = 0
var SpanMu sync.Mutex

type CrossTransfer struct {
	txIndex string
	txId    []byte
	value   []byte
	toChain uint32
	height  uint64
}

func (this *CrossTransfer) Serialization(sink *common.ZeroCopySink) {
	sink.WriteString(this.txIndex)
	sink.WriteVarBytes(this.txId)
	sink.WriteVarBytes(this.value)
	sink.WriteUint32(this.toChain)
	sink.WriteUint64(this.height)
}

func (this *CrossTransfer) Deserialization(source *common.ZeroCopySource) error {
	txIndex, eof := source.NextString()
	if eof {
		return fmt.Errorf("Waiting deserialize txIndex error")
	}
	txId, eof := source.NextVarBytes()
	if eof {
		return fmt.Errorf("Waiting deserialize txId error")
	}
	value, eof := source.NextVarBytes()
	if eof {
		return fmt.Errorf("Waiting deserialize value error")
	}
	toChain, eof := source.NextUint32()
	if eof {
		return fmt.Errorf("Waiting deserialize toChain error")
	}
	height, eof := source.NextUint64()
	if eof {
		return fmt.Errorf("Waiting deserialize height error")
	}
	this.txIndex = txIndex
	this.txId = txId
	this.value = value
	this.toChain = toChain
	this.height = height
	return nil
}

type EthereumManager struct {
	config         *config.ServiceConfig
	restClient     *tools.RestClient
	client         *ethclient.Client
	currentHeight  uint64
	forceHeight    uint64
	lockerContract *bind.BoundContract
	polySdk        *sdkp.PolySdk
	polySigner     *sdk.Account
	exitChan       chan int
	header4sync    [][]byte
	crosstx4sync   []*CrossTransfer
	db             *db.BoltDB

	TendermintClient *TendermintClient

	LastSpanId   uint64
	LastSpanId2  uint64
}

func NewEthereumManager(servconfig *config.ServiceConfig, startheight uint64, startforceheight uint64, ontsdk *sdkp.PolySdk, client *ethclient.Client,
	boltDB *db.BoltDB,
	tendermintRPCURL string,
	cdc *codec.Codec,
	tclientHttp *rpcclient.HTTP) (*EthereumManager, error) {
	var wallet *sdk.Wallet
	var err error
	if !common.FileExisted(servconfig.PolyConfig.WalletFile) {
		wallet, err = ontsdk.CreateWallet(servconfig.PolyConfig.WalletFile)
		if err != nil {
			return nil, err
		}
	} else {
		wallet, err = ontsdk.OpenWallet(servconfig.PolyConfig.WalletFile)
		if err != nil {
			log.Errorf("NewETHManager - wallet open error: %s", err.Error())
			return nil, err
		}
	}
	signer, err := wallet.GetDefaultAccount([]byte(servconfig.PolyConfig.WalletPwd))
	if err != nil || signer == nil {
		signer, err = wallet.NewDefaultSettingAccount([]byte(servconfig.PolyConfig.WalletPwd))
		if err != nil {
			log.Errorf("NewETHManager - wallet password error")
			return nil, err
		}

		err = wallet.Save()
		if err != nil {
			return nil, err
		}
	}
	log.Infof("NewETHManager - poly address: %s", signer.Address.ToBase58())

	tclient, err := NewTendermintClient(tendermintRPCURL, boltDB, cdc, tclientHttp)
	if err != nil {
		log.Errorf("ethereummanager.New - NewTendermintClient error, address: %s, error: %s", tendermintRPCURL, err.Error())
	}

	mgr := &EthereumManager{
		config:           servconfig,
		exitChan:         make(chan int),
		currentHeight:    startheight,
		forceHeight:      startforceheight,
		restClient:       tools.NewRestClient(),
		client:           client,
		polySdk:          ontsdk,
		polySigner:       signer,
		header4sync:      make([][]byte, 0),
		crosstx4sync:     make([]*CrossTransfer, 0),
		db:               boltDB,
		TendermintClient: tclient,
	}
	err = mgr.init()
	if err != nil {
		return nil, err
	} else {
		return mgr, nil
	}
}

func (this *EthereumManager) SyncHeaderToPoly() error {
	currentHeight := this.currentHeight + 1

	forceMode := false
	if this.forceHeight > 0 {
		currentHeight = this.forceHeight + 1
		forceMode = true
	}

	fetchBlockTicker := time.NewTicker(time.Duration(this.config.ETHConfig.MonitorInterval) * time.Second)
	for {
		select {
		case <-fetchBlockTicker.C:
			if !forceMode {
				// reset start
				// currentHeight = this.findLastestHeight() + 1
			}

			height, err := tools.GetNodeHeight(this.config.ETHConfig.RestURL, this.restClient)
			if err != nil {
				log.Errorf("SyncHeaderToPoly - cannot get node height, err: %w", err)
				continue
			}
			if height-currentHeight <= config.ETH_USEFUL_BLOCK_NUM {
				continue
			}

			log.Infof("SyncHeaderToPoly - eth height is %d, currentheight: %d, diff: %d", height, currentHeight, height-currentHeight)

			for currentHeight < height-config.ETH_USEFUL_BLOCK_NUM {
				err := this.handleBlockHeader(currentHeight)

				if err != nil {
					if errors.Is(err, mytypes.ErrSpanNotFound) {
						log.Warnf("SyncHeaderToPoly error - ErrSpanNotFound, the bor and spanId is too new on heimdall height, bor height: %d, error: %w", currentHeight, err)
					} else {
						log.Errorf("SyncHeaderToPoly error - handleBlockHeader error, height: %d, error: %w", currentHeight, err)
					}
					break
				}

				if len(this.header4sync) >= this.config.ETHConfig.HeadersPerBatch ||
					(currentHeight == height-config.ETH_USEFUL_BLOCK_NUM-1 && len(this.header4sync) > 0) {
					if err := this.commitHeader(&currentHeight); err != nil {
						if strings.Contains(err.Error(), "block validator is not right, next validator hash:") {
							log.Warnf("SyncHeaderToPoly commit error: %w", err)

							currentHeight = currentHeight - uint64(len(this.header4sync)) + 1
						} else if strings.Contains(err.Error(), "hard forked") || strings.Contains(err.Error(), "missing required field") {
							log.Errorf("SyncHeaderToPoly commit err: %s", err)

							this.rollBackToCommAncestor(&currentHeight)
						} else if strings.Contains(err.Error(), "data outdated") || strings.Contains(err.Error(), "go to future") {
							log.Warnf("SyncHeaderToPoly commit err: %s", err)

							currentHeight = this.findLastestHeight() + 1
						} else {
							log.Errorf("SyncHeaderToPoly commit error: %w", err)

							currentHeight = currentHeight - uint64(len(this.header4sync)) + 1
						}

						// currentHeight = currentHeight - uint64(len(this.header4sync)) + 1
						this.header4sync = make([][]byte, 0)

						break
					}
					this.LastSpanId = this.LastSpanId2

					this.header4sync = make([][]byte, 0)
				}

				currentHeight++
			}

		case <-this.exitChan:
			return nil
		}
	}
}

func (this *EthereumManager) rollBackToCommAncestor(currentHeight *uint64) {
	log.Infof("rollBackToCommAncestor - hard fork, start height: %d", *currentHeight)
	for ; ; *currentHeight-- {
		raw, err := this.polySdk.GetStorage(autils.HeaderSyncContractAddress.ToHexString(),
			append(append([]byte(scom.MAIN_CHAIN), autils.GetUint64Bytes(this.config.ETHConfig.SideChainId)...), autils.GetUint64Bytes(*currentHeight)...))
		if len(raw) == 0 || err != nil {
			continue
		}
		hdr, err := this.client.HeaderByNumber(context.Background(), big.NewInt(int64(*currentHeight)))
		if err != nil {
			log.Errorf("rollBackToCommAncestor - failed to get header by number, so we wait for one second to retry: %v", err)
			time.Sleep(time.Second)
			*currentHeight++
			continue
		}
		log.Infof("rollBackToCommAncestor - hard fork, check, currentHeight: %d, currentHash: %s, get hash: %s", *currentHeight, hexutil.Encode(raw[:]), hdr.Hash().String())
		if bytes.Equal(hdr.Hash().Bytes(), raw) {
			log.Infof("rollBackToCommAncestor - find the common ancestor: %s(number: %d)", hdr.Hash().String(), *currentHeight)
			break
		}
	}
}

func (this *EthereumManager) SyncEventToPoly() error {
	currentHeight := this.currentHeight

	fetchBlockTicker := time.NewTicker(time.Duration(this.config.ETHConfig.MonitorInterval) * time.Second)

	for {
		select {
		case <-fetchBlockTicker.C:
			// currentHeight = this.findLastestHeight()

			height, err := tools.GetNodeHeight(this.config.ETHConfig.RestURL, this.restClient)
			if err != nil {
				log.Errorf("SyncEventToPoly - cannot get node height, err: %w", err)
				continue
			}
			if height-currentHeight <= config.ETH_USEFUL_BLOCK_NUM {
				continue
			}
			log.Infof("SyncEventToPoly - eth height is %d", height)

			for currentHeight < height-config.ETH_USEFUL_BLOCK_NUM {
				if currentHeight%10 == 0 {
					log.Infof("SyncEventToPoly - handle confirmed eth Block height: %d", currentHeight)
				}

				ret := this.fetchLockDepositEvents(currentHeight, this.client)

				if !ret {
					log.Errorf("SyncEventToPoly - fetchLockDepositEvents on height :%d failed", height)
				}

				currentHeight++
			}

		case <-this.exitChan:
			return nil
		}
	}

}

func (this *EthereumManager) MonitorChain() {
	go this.SyncHeaderToPoly()
	go this.SyncEventToPoly()
}

func (this *EthereumManager) init() error {
	// get latest height
	latestHeight := this.findLastestHeight()
	if latestHeight == 0 {
		return fmt.Errorf("init - the genesis block has not synced!")
	}
	if this.forceHeight > 0 && this.forceHeight < latestHeight {
		this.currentHeight = this.forceHeight
	} else {
		this.currentHeight = latestHeight
	}
	log.Infof("EthereumManager init - start height: %d", this.currentHeight)
	return nil
}

func (this *EthereumManager) findLastestHeight() uint64 {
	// try to get key
	var sideChainIdBytes [8]byte
	binary.LittleEndian.PutUint64(sideChainIdBytes[:], this.config.ETHConfig.SideChainId)
	contractAddress := autils.HeaderSyncContractAddress
	key := append([]byte(scom.CURRENT_HEADER_HEIGHT), sideChainIdBytes[:]...)
	// try to get storage
	result, err := this.polySdk.GetStorage(contractAddress.ToHexString(), key)
	if err != nil {
		return 0
	}
	if len(result) == 0 {
		return 0
	} else {
		return binary.LittleEndian.Uint64(result)
	}
}

func (this *EthereumManager) makeHeaderWithOptionalProof(height uint64, eth *ethtypes.Header) (*types.HeaderWithOptionalProof, error) {
	headerWithOptionalProof := &types.HeaderWithOptionalProof{}
	headerWithOptionalProof.Header = *eth
	headerWithOptionalProof.Proof = nil

	// construct bor headers with proof to poly
	// only sprint end needs to send proof
	isSprintEnd := (eth.Number.Uint64()+1)%Sprint == 0
	if !isSprintEnd {
		return headerWithOptionalProof, nil
	}

	// Polygon: add heimdall header and proof
	hHeight, err := service.GetBestCosmosHeightForBor()
	if err != nil {
		return nil, err
	}

	// get span data, heimdall header, proof
	spanId, err := this.TendermintClient.GetSpanIdByBor(height)
	if err != nil {
		return nil, err
	}
	if spanId == 0 {
		return nil, fmt.Errorf("ethereummanager.handleBlockHeader - db getSpanId not found, on height :%d failed", height)
	}

	/* 	borhOnPoly := this.findLastestHeight()
		borhOnPolySpanId, err := this.TendermintClient.GetSpanIdByBor(borhOnPoly)
		if err != nil {
			return nil, err
		}
		if borhOnPolySpanId == 0 {
			return nil, fmt.Errorf("ethereummanager.handleBlockHeader - db getSpanId not found, on height :%d failed", height)
		}
		*/

	 	latestSpan, err := this.TendermintClient.GetLatestSpan(hHeight)
		if err != nil {
			return nil, fmt.Errorf("GetLatestSpan error - %w", err)
		}
		latestSpanId := latestSpan.ID

		if spanId == this.LastSpanId && spanId != latestSpanId {
			return headerWithOptionalProof, nil
		}

	spanRes, _, err := this.TendermintClient.GetSpanRes(spanId, hHeight-1)
	if err != nil {
		return nil, fmt.Errorf("ethereummanager.handleBlockHeader - tendermintClient.GetSpan error, on hHeight :%d, id: %d, error: %w",
			hHeight-1, spanId, err)
	}

	// get heimdall header, proof
	cosmosHeader, err := this.TendermintClient.GetCosmosHdr(hHeight)
	if err != nil {
		return nil, err
	}

	// construct bor headers with proof to poly
	// only sprint end needs to send proof
	cosmosProof := &types.CosmosProof{}

	cosmosProofValue := &types.CosmosProofValue{}
	kp := merkle.KeyPath{}
	kp = kp.AppendKey([]byte("bor"), merkle.KeyEncodingURL)
	kp = kp.AppendKey(spanRes.Key, merkle.KeyEncodingURL)

	cosmosProofValue.Kp = kp.String()
	cosmosProofValue.Value = spanRes.GetValue()

	cosmosProof.Value = *cosmosProofValue
	cosmosProof.Proof = *spanRes.Proof
	cosmosProof.Header = *cosmosHeader

	headerWithOptionalProof.Proof, err = this.TendermintClient.Codec.MarshalBinaryBare(cosmosProof)
	if err != nil {
		return nil, err
	}

	this.LastSpanId2 = spanId

	return headerWithOptionalProof, nil
}

func (this *EthereumManager) handleBlockHeader(height uint64) error {

	hdreth, err := this.client.HeaderByNumber(context.Background(), big.NewInt(int64(height)))
	if err != nil {
		return fmt.Errorf("handleBlockHeader - GetNodeHeader on height: %d failed, error: %w", height, err)
	}

	hdr, err := this.makeHeaderWithOptionalProof(height, hdreth)
	if err != nil {
		return fmt.Errorf("handleBlockHeader - makeHeaderWithOptionalProof error on height :%d failed, error: %w",
			height, err)
	}

	rawHdr, _ := json.Marshal(hdr)
	log.Infof("handleBlockHeader - makeHeaderWithOptionalProof height: %d", height)

	rawPolyHdr, _ := this.polySdk.GetStorage(autils.HeaderSyncContractAddress.ToHexString(),
		append(append([]byte(scom.MAIN_CHAIN), autils.GetUint64Bytes(this.config.ETHConfig.SideChainId)...), autils.GetUint64Bytes(height)...))
	if len(rawPolyHdr) == 0 || !bytes.Equal(rawPolyHdr, hdr.Hash().Bytes()) {
		this.header4sync = append(this.header4sync, rawHdr)
	}
	return nil
}

func (this *EthereumManager) fetchLockDepositEvents(height uint64, client *ethclient.Client) bool {
	lockAddress := ethcommon.HexToAddress(this.config.ETHConfig.ECCMContractAddress)
	lockContract, err := eccm_abi.NewEthCrossChainManager(lockAddress, client)
	if err != nil {
		return false
	}
	opt := &bind.FilterOpts{
		Start:   height,
		End:     &height,
		Context: context.Background(),
	}
	events, err := lockContract.FilterCrossChainEvent(opt, nil)
	if err != nil {
		log.Errorf("fetchLockDepositEvents - FilterCrossChainEvent error :%s", err.Error())
		return false
	}
	if events == nil {
		log.Infof("fetchLockDepositEvents - no events found on FilterCrossChainEvent")
		return false
	}

	for events.Next() {
		evt := events.Event
		var isTarget bool

		if len(this.config.TargetContracts) > 0 {
			toContractStr := evt.ProxyOrAssetContract.String()
			for _, v := range this.config.TargetContracts {
				toChainIdArr, ok := v[toContractStr]
				if ok {
					if len(toChainIdArr["outbound"]) == 0 {
						isTarget = true
						break
					}
					for _, id := range toChainIdArr["outbound"] {
						if id == evt.ToChainId {
							isTarget = true
							break
						}
					}
					if isTarget {
						break
					}
				}
			}
			if !isTarget {
				continue
			}
		}

		param := &common2.MakeTxParam{}
		_ = param.Deserialization(common.NewZeroCopySource([]byte(evt.Rawdata)))

		raw, _ := this.polySdk.GetStorage(autils.CrossChainManagerContractAddress.ToHexString(),
			append(append([]byte(cross_chain_manager.DONE_TX), autils.GetUint64Bytes(this.config.ETHConfig.SideChainId)...), param.CrossChainID...))
		if len(raw) != 0 {
			log.Debugf("fetchLockDepositEvents - ccid %s (tx_hash: %s) already on poly",
				hex.EncodeToString(param.CrossChainID), evt.Raw.TxHash.Hex())
			continue
		}

		index := big.NewInt(0)
		index.SetBytes(evt.TxId)
		crossTx := &CrossTransfer{
			txIndex: tools.EncodeBigInt(index),
			txId:    evt.Raw.TxHash.Bytes(),
			toChain: uint32(evt.ToChainId),
			value:   []byte(evt.Rawdata),
			height:  height,
		}
		sink := common.NewZeroCopySink(nil)
		crossTx.Serialization(sink)
		err = this.db.PutRetry(sink.Bytes())
		if err != nil {
			log.Errorf("fetchLockDepositEvents - this.db.PutRetry error: %s", err)
		}
		log.Infof("fetchLockDepositEvent -  height: %d", height)
	}
	return true
}

func (this *EthereumManager) commitHeader(currentHeight *uint64) error {
	log.Infof("commitHeader bor start - send transaction to poly chain len %d", len(this.header4sync))

	snycheightLast := this.findLastestHeight()

	lenh := len(this.header4sync)
	tx, err := this.polySdk.Native.Hs.SyncBlockHeader(
		this.config.ETHConfig.SideChainId,
		this.polySigner.Address,
		this.header4sync,
		this.polySigner,
	)
	if err != nil {
		return fmt.Errorf("commitHeader bor - send transaction to poly chain err, currentHeight: %d, restart from %d, error: %w",
			*currentHeight, *currentHeight-uint64(lenh), err)
	}

	tick := time.NewTicker(50 * time.Millisecond)
	var h uint32
	for range tick.C {
		h, _ = this.polySdk.GetBlockHeightByTxHash(tx.ToHexString())
		curr, _ := this.polySdk.GetCurrentBlockHeight()
		if h > 0 && curr > h {
			break
		}
	}

	snycheight := this.findLastestHeight()
	height, err := tools.GetNodeHeight(this.config.ETHConfig.RestURL, this.restClient)
	if err != nil {
		return fmt.Errorf("tools.GetNodeHeight error: %w", err)
	}

	currentHeightStart := *currentHeight - uint64(lenh) + 1

	if snycheight == snycheightLast {
		// outdated
		if *currentHeight < snycheight {
			if this.forceHeight == 0 { // this is force mode, not a error
				return fmt.Errorf("commitHeader bor failed, poly bor height not updated, data outdated, send transaction %s, last bor height %d, current bor height %d, input currentHeight: %d currentStart: %d",
					tx.ToHexString(), snycheightLast, snycheight, *currentHeight, currentHeightStart)
			}

		} else if (*currentHeight - uint64(lenh) + 1) > (snycheightLast + 1) { // go to future
			return fmt.Errorf("commitHeader bor failed, poly bor height not updated, go to future, send transaction %s, last bor height %d, current bor height %d, input currentHeight: %d, currentStart: %d",
				tx.ToHexString(), snycheightLast, snycheight, *currentHeight, currentHeightStart)
		} else { // may be hard forked
			return fmt.Errorf("commitHeader bor failed, poly bor height not updated, maybe hard forked, send transaction %s, last bor height %d, current bor height %d, input currentHeight: %d, currentStart: %d",
				tx.ToHexString(), snycheightLast, snycheight, *currentHeight, currentHeightStart)
		}

		/* if this.forceHeight == 0 { // not force mode
			return fmt.Errorf("commitHeader bor failed, poly bor height not updated, send transaction %s, last bor height %d, current bor height %d, input currentHeight: %d",
				tx.ToHexString(), snycheightLast, snycheight, *currentHeight)
		} */
	}

	log.Infof("commitHeader bor success - send transaction %s to poly chain and confirmed on poly height %d, snyced bor height: %d, lastest bor height: %d, diff: %d",
		tx.ToHexString(), h, snycheight, height, height-snycheight)

	StartFirst = false

	return nil
}

func (this *EthereumManager) MonitorDeposit() {
	monitorTicker := time.NewTicker(time.Duration(this.config.ETHConfig.MonitorInterval) * time.Second)
	for {
		select {
		case <-monitorTicker.C:
			height, err := tools.GetNodeHeight(this.config.ETHConfig.RestURL, this.restClient)
			if err != nil {
				log.Errorf("MonitorDeposit - cannot get eth node height, err: %s", err)
				continue
			}
			snycheight := this.findLastestHeight()
			log.Infof("MonitorDeposit from eth - snyced bor height: %d, lastest bor height: %d, diff: %d", snycheight, height, height-snycheight)

			// change 120 blocks
			err2 := this.handleLockDepositEvents(snycheight)
			if err2 != nil {
				log.Errorf("MonitorDeposit from eth - handleLockDepositEvents error, err: %w", err2)
			}
		case <-this.exitChan:
			return
		}
	}
}
func (this *EthereumManager) handleLockDepositEvents(refHeight uint64) error {
	retryList, err := this.db.GetAllRetry()
	if err != nil {
		return fmt.Errorf("handleLockDepositEvents - this.db.GetAllRetry error, refHeight: %d, error: %w", refHeight, err)
	}
	for _, v := range retryList {
		time.Sleep(time.Second * 1)
		crosstx := new(CrossTransfer)
		err := crosstx.Deserialization(common.NewZeroCopySource(v))
		if err != nil {
			log.Errorf("handleLockDepositEvents - retry.Deserialization error, refHeight: %d, error: %s", refHeight, err)
			continue
		}
		//1. decode events
		key := crosstx.txIndex
		keyBytes, err := eth.MappingKeyAt(key, "01")
		if err != nil {
			log.Errorf("handleLockDepositEvents - MappingKeyAt error, refHeight: %d, error:%s", refHeight, err.Error())
			continue
		}
		if refHeight <= crosstx.height+this.config.ETHConfig.BlockConfig {
			continue
		}
		height := int64(refHeight - this.config.ETHConfig.BlockConfig)
		heightHex := hexutil.EncodeBig(big.NewInt(height))
		proofKey := hexutil.Encode(keyBytes)
		//2. get proof
		proof, err := tools.GetProof(this.config.ETHConfig.RestURL, this.config.ETHConfig.ECCDContractAddress, proofKey, heightHex, this.restClient)
		if err != nil {
			log.Errorf("handleLockDepositEvents - tools.GetProof error, proof height: %d, refHeight: %d, error :%w", height, refHeight, err)
			continue
		}
		//3. commit proof to poly
		txHash, err := this.commitProof(uint32(height), proof, crosstx.value, crosstx.txId)
		// log.Infof("noCheckFees params send to poly: height: %d, txId: %s, poly hash: %s", height, hex.EncodeToString(crosstx.txId), txHash)
		if err != nil {
			if strings.Contains(err.Error(), "chooseUtxos, current utxo is not enough") {
				log.Infof("handleLockDepositEvents - invokeNativeContract error, refHeight: %d, error: %s", refHeight, err)
				continue
			} else {
				if err := this.db.DeleteRetry(v); err != nil {
					log.Errorf("handleLockDepositEvents - this.db.DeleteRetry error, refHeight: %d, error: %s", refHeight, err)
				}
				if strings.Contains(err.Error(), "tx already done") {
					log.Debugf("handleLockDepositEvents - eth_tx %s already on poly, refHeight: %d", ethcommon.BytesToHash(crosstx.txId).String(), refHeight)
				} else {
					log.Errorf("handleLockDepositEvents - invokeNativeContract error, refHeight: %d, for eth_tx %s: %s", refHeight, ethcommon.BytesToHash(crosstx.txId).String(), err)
				}
				continue
			}
		}
		//4. put to check db for checking
		err = this.db.PutCheck(txHash, v)
		if err != nil {
			log.Errorf("handleLockDepositEvents - this.db.PutCheck error: %s", err)
		}
		err = this.db.DeleteRetry(v)
		if err != nil {
			log.Errorf("handleLockDepositEvents - this.db.PutCheck error: %s", err)
		}
		log.Infof("handleLockDepositEvents - syncProofToAlia txHash is %s, refHeight: %d", txHash, refHeight)
	}
	return nil
}

func (this *EthereumManager) commitProof(height uint32, proof []byte, value []byte, txhash []byte) (string, error) {
	log.Debugf("commit proof, height: %d, proof: %s, value: %s, txhash: %s", height, string(proof), hex.EncodeToString(value), hex.EncodeToString(txhash))
	tx, err := this.polySdk.Native.Ccm.ImportOuterTransfer(
		this.config.ETHConfig.SideChainId,
		value,
		height,
		proof,
		ethcommon.Hex2Bytes(this.polySigner.Address.ToHexString()),
		[]byte{},
		this.polySigner)
	if err != nil {
		return "", err
	} else {
		log.Infof("commitProof - send transaction to poly chain: this.config.ETHConfig.SideChainId: %d, height: %d, polytx: %s",
			this.config.ETHConfig.SideChainId,
			height,
			tx.ToHexString())
		return tx.ToHexString(), nil
	}
}
func (this *EthereumManager) parserValue(value []byte) []byte {
	source := common.NewZeroCopySource(value)
	txHash, eof := source.NextVarBytes()
	if eof {
		fmt.Printf("parserValue - deserialize txHash error")
	}
	return txHash
}
func (this *EthereumManager) CheckDeposit() {
	checkTicker := time.NewTicker(time.Duration(this.config.ETHConfig.MonitorInterval) * time.Second)
	for {
		select {
		case <-checkTicker.C:
			this.checkLockDepositEvents()
		case <-this.exitChan:
			return
		}
	}
}
func (this *EthereumManager) checkLockDepositEvents() error {
	checkMap, err := this.db.GetAllCheck()
	if err != nil {
		return fmt.Errorf("checkLockDepositEvents - this.db.GetAllCheck error: %s", err)
	}
	for k, v := range checkMap {
		event, err := this.polySdk.GetSmartContractEvent(k)
		if err != nil {
			log.Errorf("checkLockDepositEvents - this.aliaSdk.GetSmartContractEvent error: %s", err)
			continue
		}
		if event == nil {
			continue
		}
		if event.State != 1 {
			log.Infof("checkLockDepositEvents - state of poly tx %s is not success", k)
			err := this.db.PutRetry(v)
			if err != nil {
				log.Errorf("checkLockDepositEvents - this.db.PutRetry error:%s", err)
			}
		}
		err = this.db.DeleteCheck(k)
		if err != nil {
			log.Errorf("checkLockDepositEvents - this.db.DeleteRetry error:%s", err)
		}
	}
	return nil
}
