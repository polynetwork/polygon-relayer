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
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/ethereum/go-ethereum/accounts"
	"github.com/ethereum/go-ethereum/accounts/abi"
	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ontio/ontology-crypto/keypair"
	"github.com/ontio/ontology-crypto/signature"
	"github.com/polynetwork/eth-contracts/go_abi/eccd_abi"
	"github.com/polynetwork/eth-contracts/go_abi/eccm_abi"
	"github.com/polynetwork/poly/common"
	"github.com/polynetwork/poly/common/password"
	vconfig "github.com/polynetwork/poly/consensus/vbft/config"
	common2 "github.com/polynetwork/poly/native/service/cross_chain_manager/common"
	"github.com/polynetwork/polygon-relayer/config"
	"github.com/polynetwork/polygon-relayer/db"
	"github.com/polynetwork/polygon-relayer/log"
	sdk "github.com/polynetwork/polygon-relayer/poly_go_sdk"

	"math/big"
	"time"

	"poly_bridge_sdk"

	"github.com/ethereum/go-ethereum"
	"github.com/polynetwork/polygon-relayer/tools"

	polytypes "github.com/polynetwork/poly/core/types"
)

const (
	ChanLen = 0
)

const (
	FEE_NOCHECK = iota
	FEE_HASPAY
	FEE_NOTPAY
)

type BridgeTransaction struct {
	header       *polytypes.Header
	param        *common2.ToMerkleValue
	headerProof  string
	anchorHeader *polytypes.Header
	polyTxHash   string
	rawAuditPath []byte
	hasPay       uint8
	fee          string
}

func (this *BridgeTransaction) Serialization(sink *common.ZeroCopySink) {
	this.header.Serialization(sink)
	this.param.Serialization(sink)
	if this.headerProof != "" && this.anchorHeader != nil {
		sink.WriteUint8(1)
		sink.WriteString(this.headerProof)
		this.anchorHeader.Serialization(sink)
	} else {
		sink.WriteUint8(0)
	}
	sink.WriteString(this.polyTxHash)
	sink.WriteVarBytes(this.rawAuditPath)
	sink.WriteUint8(this.hasPay)
	sink.WriteString(this.fee)
}

func (this *BridgeTransaction) Deserialization(source *common.ZeroCopySource) error {
	this.header = new(polytypes.Header)
	err := this.header.Deserialization(source)
	if err != nil {
		return err
	}
	this.param = new(common2.ToMerkleValue)
	err = this.param.Deserialization(source)
	if err != nil {
		return err
	}
	anchor, eof := source.NextUint8()
	if eof {
		return fmt.Errorf("Waiting deserialize anchor error")
	}
	if anchor == 1 {
		this.headerProof, eof = source.NextString()
		if eof {
			return fmt.Errorf("Waiting deserialize header proof error")
		}
		this.anchorHeader = new(polytypes.Header)
		this.anchorHeader.Deserialization(source)
	}
	this.polyTxHash, eof = source.NextString()
	if eof {
		return fmt.Errorf("Waiting deserialize poly tx hash error")
	}
	this.rawAuditPath, eof = source.NextVarBytes()
	if eof {
		return fmt.Errorf("Waiting deserialize poly tx hash error")
	}
	this.hasPay, eof = source.NextUint8()
	if eof {
		return fmt.Errorf("Waiting deserialize has pay error")
	}
	this.fee, eof = source.NextString()
	if eof {
		return fmt.Errorf("Waiting deserialize fee error")
	}
	return nil
}

type BridgeTransactionAndHash struct {
	BridgeTransaction *BridgeTransaction
	Hash              string
}

type PolyManager struct {
	config        *config.ServiceConfig
	polySdk       *sdk.PolySdk
	currentHeight uint32
	contractAbi   *abi.ABI
	exitChan      chan int
	db            *db.BoltDB
	ethClient     *ethclient.Client
	senders       []*EthSender
	bridgeSdk     *poly_bridge_sdk.BridgeFeeCheck
	eccdInstance  *eccd_abi.EthCrossChainData
	nofeemode     bool

	txChan    chan *BridgeTransactionAndHash
	txSenChan chan *EthSender
	txLock    *sync.Mutex
}

func NewPolyManager(servCfg *config.ServiceConfig,
	startblockHeight uint32,
	polySdk *sdk.PolySdk,
	ethereumsdk *ethclient.Client,
	boltDB *db.BoltDB,
	nofeemode bool) (polyManager *PolyManager, err error) {
	contractabi, err := abi.JSON(strings.NewReader(eccm_abi.EthCrossChainManagerABI))
	if err != nil {
		return nil, err
	}
	address := ethcommon.HexToAddress(servCfg.ETHConfig.ECCDContractAddress)
	instance, err := eccd_abi.NewEthCrossChainData(address, ethereumsdk)
	if err != nil {
		return nil, err
	}
	chainId, err := ethereumsdk.ChainID(context.Background())
	if err != nil {
		return nil, err
	}
	ks := tools.NewEthKeyStore(servCfg.ETHConfig, chainId)
	accArr := ks.GetAccounts()
	if len(servCfg.ETHConfig.KeyStorePwdSet) == 0 {
		fmt.Println("please input the passwords for ethereum keystore: ")
		for _, v := range accArr {
			fmt.Printf("For address %s. ", v.Address.String())
			raw, err := password.GetPassword()
			if err != nil {
				log.Fatalf("failed to input password: %v", err)
				panic(err)
			}
			servCfg.ETHConfig.KeyStorePwdSet[strings.ToLower(v.Address.String())] = string(raw)
		}
	}
	if err = ks.UnlockKeys(servCfg.ETHConfig); err != nil {
		return nil, err
	}

	senders := make([]*EthSender, len(accArr))
	for i, v := range senders {
		v = &EthSender{}
		v.acc = accArr[i]

		v.ethClient = ethereumsdk
		v.keyStore = ks
		v.config = servCfg
		v.polySdk = polySdk
		v.contractAbi = &contractabi
		v.nonceManager = tools.NewNonceManager(ethereumsdk)
		v.cmap = make(map[string]chan *EthTxInfo)
		v.result = make(chan bool)
		v.locked = false
		v.id = i
		v.eccdInstance = instance

		senders[i] = v
	}
	txSenChan := make(chan *EthSender, len(senders))
	for _, v := range senders {
		txSenChan <- v
	}

	bridgeSdk := poly_bridge_sdk.NewBridgeFeeCheck(servCfg.BridgeUrl, 5)
	return &PolyManager{
		exitChan:      make(chan int),
		config:        servCfg,
		polySdk:       polySdk,
		currentHeight: startblockHeight,
		contractAbi:   &contractabi,
		db:            boltDB,
		ethClient:     ethereumsdk,
		senders:       senders,
		bridgeSdk:     bridgeSdk,
		eccdInstance:  instance,

		nofeemode: nofeemode,

		txChan:    make(chan *BridgeTransactionAndHash, 4),
		txSenChan: txSenChan,
		txLock:   &sync.Mutex{},
		
	}, nil
}

func (this *PolyManager) findLatestHeight() uint32 {
	height, err := this.eccdInstance.GetCurEpochStartHeight(nil)
	if err != nil {
		log.Errorf("findLatestHeight - GetLatestHeight failed: %s", err.Error())
		return 0
	}
	return uint32(height)
}

func (this *PolyManager) init() bool {
	if this.currentHeight > 0 {
		log.Infof("PolyManager init - start height from flag: %d", this.currentHeight)
		return true
	}
	this.currentHeight = this.db.GetPolyHeight()
	latestHeight := this.findLatestHeight()
	if latestHeight > this.currentHeight {
		this.currentHeight = latestHeight
		log.Infof("PolyManager init - latest height from ECCM: %d", this.currentHeight)
		return true
	}
	log.Infof("PolyManager init - latest height from DB: %d", this.currentHeight)

	return true
}

func (this *PolyManager) MonitorChain() {
	ret := this.init()
	if ret == false {
		log.Errorf("MonitorChain - init failed\n")
	}
	monitorTicker := time.NewTicker(config.ETH_MONITOR_INTERVAL)
	var blockHandleResult bool
	for {
		select {
		case <-monitorTicker.C:
			latestheight, err := this.polySdk.GetCurrentBlockHeight()
			if err != nil {
				log.Errorf("MonitorChain - get poly chain block height error: %s", err)
				continue
			}
			latestheight--
			if latestheight-this.currentHeight < config.ONT_USEFUL_BLOCK_NUM {
				continue
			}
			log.Infof("MonitorChain - poly chain current height: %d", latestheight)
			blockHandleResult = true
			for this.currentHeight <= latestheight-config.ONT_USEFUL_BLOCK_NUM {
				blockHandleResult = this.handleDepositEvents(this.currentHeight)
				if blockHandleResult == false {
					break
				}
				this.currentHeight++
			}
			if err = this.db.UpdatePolyHeight(this.currentHeight - 1); err != nil {
				log.Errorf("MonitorChain - failed to save height of poly: %v", err)
			}
		case <-this.exitChan:
			return
		}
	}
}

func (this *PolyManager) IsEpoch(hdr *polytypes.Header) (bool, []byte, error) {
	blkInfo := &vconfig.VbftBlockInfo{}
	if err := json.Unmarshal(hdr.ConsensusPayload, blkInfo); err != nil {
		return false, nil, fmt.Errorf("commitHeader - unmarshal blockInfo error: %s", err)
	}
	if hdr.NextBookkeeper == common.ADDRESS_EMPTY || blkInfo.NewChainConfig == nil {
		return false, nil, nil
	}
	rawKeepers, err := this.eccdInstance.GetCurEpochConPubKeyBytes(nil)
	if err != nil {
		return false, nil, fmt.Errorf("failed to get current epoch keepers: %v", err)
	}

	var bookkeepers []keypair.PublicKey
	for _, peer := range blkInfo.NewChainConfig.Peers {
		keystr, _ := hex.DecodeString(peer.ID)
		key, _ := keypair.DeserializePublicKey(keystr)
		bookkeepers = append(bookkeepers, key)
	}
	bookkeepers = keypair.SortPublicKeys(bookkeepers)
	publickeys := make([]byte, 0)
	sink := common.NewZeroCopySink(nil)
	sink.WriteUint64(uint64(len(bookkeepers)))
	for _, key := range bookkeepers {
		raw := tools.GetNoCompresskey(key)
		publickeys = append(publickeys, raw...)
		sink.WriteVarBytes(crypto.Keccak256(tools.GetEthNoCompressKey(key)[1:])[12:])
	}
	if bytes.Equal(rawKeepers, sink.Bytes()) {
		return false, nil, nil
	}
	return true, publickeys, nil
}

func (this *PolyManager) handleDepositEvents(height uint32) bool {
	lastEpoch := this.findLatestHeight()
	hdr, err := this.polySdk.GetHeaderByHeight(height + 1)
	if err != nil {
		log.Errorf("handleDepositEvents - GetNodeHeader on height :%d failed", height)
		return false
	}
	isCurr := lastEpoch < height+1
	isEpoch, pubkList, err := this.IsEpoch(hdr)
	if err != nil {
		log.Errorf("falied to check isEpoch: %v", err)
		return false
	}
	var (
		anchor *polytypes.Header
		hp     string
	)
	if !isCurr {
		anchor, _ = this.polySdk.GetHeaderByHeight(lastEpoch + 1)
		proof, _ := this.polySdk.GetMerkleProof(height+1, lastEpoch+1)
		hp = proof.AuditPath
	} else if isEpoch {
		anchor, _ = this.polySdk.GetHeaderByHeight(height + 2)
		proof, _ := this.polySdk.GetMerkleProof(height+1, height+2)
		hp = proof.AuditPath
	}

	cnt := 0
	events, err := this.polySdk.GetSmartContractEventByBlock(height)
	for err != nil {
		log.Errorf("handleDepositEvents - get block event at height:%d error: %s", height, err.Error())
		return false
	}
	for _, event := range events {
		for _, notify := range event.Notify {
			if notify.ContractAddress == this.config.PolyConfig.EntranceContractAddress {
				states := notify.States.([]interface{})
				method, _ := states[0].(string)
				if method != "makeProof" {
					continue
				}
				if uint64(states[2].(float64)) != this.config.ETHConfig.SideChainId {
					continue
				}
				proof, err := this.polySdk.GetCrossStatesProof(hdr.Height-1, states[5].(string))
				if err != nil {
					log.Errorf("handleDepositEvents - failed to get proof for key %s: %v", states[5].(string), err)
					continue
				}
				auditpath, _ := hex.DecodeString(proof.AuditPath)
				value, _, _, _ := tools.ParseAuditpath(auditpath)
				param := &common2.ToMerkleValue{}
				if err := param.Deserialization(common.NewZeroCopySource(value)); err != nil {
					log.Errorf("handleDepositEvents - failed to deserialize MakeTxParam (value: %x, err: %v)", value, err)
					continue
				}
				var isTarget bool
				if len(this.config.TargetContracts) > 0 {
					toContractStr := ethcommon.BytesToAddress(param.MakeTxParam.ToContractAddress).String()
					for _, v := range this.config.TargetContracts {
						toChainIdArr, ok := v[toContractStr]
						if ok {
							if len(toChainIdArr["inbound"]) == 0 {
								isTarget = true
								break
							}
							for _, id := range toChainIdArr["inbound"] {
								if id == param.FromChainID {
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
				cnt++
				//
				bridgeTransaction := &BridgeTransaction{
					header:       hdr,
					param:        param,
					headerProof:  hp,
					anchorHeader: anchor,
					polyTxHash:   event.TxHash,
					rawAuditPath: auditpath,
					hasPay:       FEE_NOCHECK,
					fee:          "0",
				}
				sink := common.NewZeroCopySink(nil)
				bridgeTransaction.Serialization(sink)
				this.db.PutBridgeTransactions(fmt.Sprintf("%d%s", param.FromChainID, hex.EncodeToString(param.MakeTxParam.TxHash)), sink.Bytes())
				log.Infof("cross chain transactions, from chain id: %d, poly tx: %s, src tx: %s",
					param.FromChainID, hex.EncodeToString(tools.HexReverse(param.TxHash)), hex.EncodeToString(param.MakeTxParam.TxHash))
				//if !sender.commitDepositEventsWithHeader(hdr, param, hp, anchor, event.TxHash, auditpath) {
				//	return false
				//}
			}
		}
	}
	if cnt == 0 && isEpoch && isCurr {
		sender := this.selectSender()
		if sender == nil {
			log.Info("There is not sender......")
			return false
		}
		return sender.commitHeader(hdr, pubkList)
	}

	return true
}

func (this *PolyManager) selectSender1() *EthSender {
	sum := big.NewInt(0)
	balArr := make([]*big.Int, len(this.senders))
	locked := make([]bool, len(this.senders))
	for i, v := range this.senders {
	RETRY:
		locked[i] = v.locked
		bal, err := v.Balance()
		if err != nil {
			log.Errorf("failed to get balance for %s: %v", v.acc.Address.String(), err)
			time.Sleep(time.Second)
			goto RETRY
		}
		sum.Add(sum, bal)
		balArr[i] = big.NewInt(sum.Int64())
	}
	sum.Rand(rand.New(rand.NewSource(time.Now().Unix())), sum)
	for i, v := range balArr {
		res := v.Cmp(sum)
		if res >= 0 && !locked[i] {
			return this.senders[i]
		}
	}
	return this.senders[0]
}

func (this *PolyManager) selectSender() *EthSender {
	return this.senders[int(rand.Uint32())%len(this.senders)]
	/* 	for _, v := range this.senders {
	   		bal, err := v.Balance()
	   		if err != nil {
	   			continue
	   		}
	   		if bal.Cmp(new(big.Int).SetUint64(100000000000000000)) == -1 {
	   			log.Errorf("account %s balance is not enough......", v.acc.Address.String())
	   			continue
	   		}
	   		if bal.Cmp(new(big.Int).SetUint64(400000000000000000)) == -1 {
	   			log.Errorf("account %s balance is not enough......", v.acc.Address.String())
	   		}
	   		if v.locked {
	   			log.Errorf("account %s has locked......", v.acc.Address.String())
	   			continue
	   		} else {
	   			return v
	   		}
	   	}
	   	return nil */
}

func (this *PolyManager) MonitorDeposit() {
	monitorTicker := time.NewTicker(config.ETH_MONITOR_INTERVAL)
	for {
		select {
		case <-monitorTicker.C:
			this.handleLockDepositEvents()
		case <-this.exitChan:
			return
		}
	}
}

func (this *PolyManager) handleLockDepositEvents() error {
	retryList, err := this.db.GetAllBridgeTransactions()
	if err != nil {
		return fmt.Errorf("handleLockDepositEvents - this.db.GetAllBridgeTransactions error: %s", err)
	}
	log.Infof("handleLockDepositEvents - start, len: %d", len(retryList))
	if len(retryList) == 0 {
		return nil
	}
	bridgeTransactions := make(map[string]*BridgeTransaction)
	for _, v := range retryList {
		bridgeTransaction := new(BridgeTransaction)
		err := bridgeTransaction.Deserialization(common.NewZeroCopySource(v))
		if err != nil {
			log.Errorf("handleLockDepositEvents - retry.Deserialization error: %s", err)
			continue
		}
		bridgeTransactions[fmt.Sprintf("%d%s", bridgeTransaction.param.FromChainID, hex.EncodeToString(bridgeTransaction.param.MakeTxParam.TxHash))] = bridgeTransaction
	}
	noCheckFees := make([]*poly_bridge_sdk.CheckFeeReq, 0)
	for _, v := range bridgeTransactions {
		if v.hasPay == FEE_NOCHECK {
			noCheckFees = append(noCheckFees, &poly_bridge_sdk.CheckFeeReq{
				ChainId: v.param.FromChainID,
				Hash:    hex.EncodeToString(v.param.MakeTxParam.TxHash),
			})
		}
	}

	var checkfeeRes string
	if len(noCheckFees) > 0 {
		checkFees, err := this.checkFee(noCheckFees)
		if err != nil {
			log.Errorf("handleLockDepositEvents - checkFee error: %s", err)
		}

		for _, checkFee := range checkFees {
			if checkFee.Error != "" {
				log.Errorf("check fee err: %s", checkFee.Error)
				continue
			}
			item, ok := bridgeTransactions[fmt.Sprintf("%d%s", checkFee.ChainId, checkFee.Hash)]
			if ok {
				if checkFee.PayState == poly_bridge_sdk.STATE_HASPAY {
					log.Infof("tx(%d,%s) has payed fee", checkFee.ChainId, checkFee.Hash)
					item.hasPay = FEE_HASPAY
					item.fee = checkFee.Amount
				} else if checkFee.PayState == poly_bridge_sdk.STATE_NOTPAY {
					log.Infof("tx(%d,%s) has not payed fee", checkFee.ChainId, checkFee.Hash)
					item.hasPay = FEE_NOTPAY
				} else {
					log.Warnf("check fee of tx(%d,%s) failed", checkFee.ChainId, checkFee.Hash)
				}
			}
		}

		bb, _ := json.Marshal(checkFees)
		checkfeeRes = string(bb)
	}
	if !this.nofeemode {
		for k, v := range bridgeTransactions {
			if v.hasPay == FEE_NOTPAY {
				log.Infof("tx (src %d, %s, poly %s) has not pay proxy fee, ignore it, payed: %s, total response: %s",
					v.param.FromChainID, hex.EncodeToString(v.param.MakeTxParam.TxHash), v.polyTxHash, v.fee, checkfeeRes)

				this.db.DeleteBridgeTransactions(k)
				delete(bridgeTransactions, k)
			}
		}
	}
	retryBridgeTransactions := make(map[string]*BridgeTransaction)

	// txChan := make(chan *BridgeTransactionAndHash, len(bridgeTransactions))
	txSend := make([][]*BridgeTransactionAndHash, len(this.senders))
	for i:=0;i<len(this.senders);i++ {
		txSend[i] = make([]*BridgeTransactionAndHash, 0)
	}

	log.Infof("wg.Wait start start 1")
	sortedTx := make([]*BridgeTransactionAndHash, len(bridgeTransactions))
	for len(bridgeTransactions) > 0 {
		var maxFeeOfTransaction *BridgeTransaction = nil
		maxFee := new(big.Float).SetUint64(0)
		maxFeeOfTxHash := ""

		log.Infof("select transaction")

		for k, v := range bridgeTransactions {

			fee, result := new(big.Float).SetString(v.fee)
			if !result {
				log.Errorf("fee is invalid")
				delete(bridgeTransactions, maxFeeOfTxHash)
				continue
			}

			if (v.hasPay == FEE_HASPAY && fee.Cmp(maxFee) > 0) || this.nofeemode {
				maxFee = fee
				maxFeeOfTransaction = v
				maxFeeOfTxHash = k
			}
		}

		if maxFeeOfTransaction != nil {

			log.Infof("select transaction, poly txhash: %s, poly txhash2: %s", maxFeeOfTransaction.polyTxHash, hex.EncodeToString(tools.HexReverse(maxFeeOfTransaction.param.TxHash)))

			/* txChan <- &BridgeTransactionAndHash{
				BridgeTransaction: maxFeeOfTransaction,
				Hash:              maxFeeOfTxHash,
			} */

			sortedTx = append(sortedTx, &BridgeTransactionAndHash{
				BridgeTransaction: maxFeeOfTransaction,
				Hash:              maxFeeOfTxHash,
			})

			delete(bridgeTransactions, maxFeeOfTxHash)

			/* 			sender := this.selectSender()
			   			if sender == nil {
			   				log.Infof("There is no sender.......")
			   				return nil
			   			}
			   			log.Infof("sender %s is handling poly tx (hash: %s)", sender.acc.Address.String(), hex.EncodeToString(tools.HexReverse(maxFeeOfTransaction.param.TxHash)))
			   			res := sender.commitDepositEventsWithHeader(maxFeeOfTransaction.header,
			   				maxFeeOfTransaction.param,
			   				maxFeeOfTransaction.headerProof,
			   				maxFeeOfTransaction.anchorHeader,
			   				hex.EncodeToString(maxFeeOfTransaction.param.TxHash),
			   				maxFeeOfTransaction.rawAuditPath)

			   			log.Infof("sender %s tx return tx (hash: %s)", sender.acc.Address.String(), hex.EncodeToString(tools.HexReverse(maxFeeOfTransaction.param.TxHash)))
			   			if res {
			   				this.db.DeleteBridgeTransactions(maxFeeOfTxHash)
			   			} else {
			   				retryBridgeTransactions[maxFeeOfTxHash] = maxFeeOfTransaction
			   			}

			   			delete(bridgeTransactions, maxFeeOfTxHash) */
		} else {
			break
		}
	}
	//close(txChan)
	for i,v := range sortedTx {
		if v != nil {
			txSend[i%len(this.senders)] = append(txSend[i%len(this.senders)], v)
			log.Infof("txSend %d, v: %w", i%len(this.senders), v)
		}
	}

	var wg sync.WaitGroup
	for i := 0; i < len(this.senders); i++ {
		wg.Add(1)
		log.Infof("wg.Wait create gorutine start %d", i)
		go func(txChan []*BridgeTransactionAndHash, txLock *sync.Mutex, sender *EthSender) {
			defer wg.Done()

			for _, maxFeeOfTransactionAndHash := range txChan {
				
				maxFeeOfTransaction := maxFeeOfTransactionAndHash.BridgeTransaction
				maxFeeOfTxHash := maxFeeOfTransactionAndHash.Hash

				// sender := <- txSenChan

				// sender := this.selectSender()
				log.Infof("sender %s is handling poly tx (hash: %s), height: %d", sender.acc.Address.String(), hex.EncodeToString(tools.HexReverse(maxFeeOfTransaction.param.TxHash)), maxFeeOfTransaction.header.Height)
				res := sender.commitDepositEventsWithHeader(maxFeeOfTransaction.header,
					maxFeeOfTransaction.param,
					maxFeeOfTransaction.headerProof,
					maxFeeOfTransaction.anchorHeader,
					hex.EncodeToString(maxFeeOfTransaction.param.TxHash),
					maxFeeOfTransaction.rawAuditPath)

				log.Infof("sender %s tx return tx (poly hash: %s)", sender.acc.Address.String(), hex.EncodeToString(tools.HexReverse(maxFeeOfTransaction.param.TxHash)))

				
				if res {
					this.db.DeleteBridgeTransactions(maxFeeOfTxHash)
				} else {
					log.Infof("sender %s txLock start  tx (poly hash: %s)", sender.acc.Address.String(), hex.EncodeToString(tools.HexReverse(maxFeeOfTransaction.param.TxHash)))
					txLock.Lock()
					retryBridgeTransactions[maxFeeOfTxHash] = maxFeeOfTransaction
					log.Infof("sender %s txLock  Unlock start tx (poly hash: %s)", sender.acc.Address.String(), hex.EncodeToString(tools.HexReverse(maxFeeOfTransaction.param.TxHash)))
					txLock.Unlock()
					log.Infof("sender %s txLock  Unlock finished tx (poly hash: %s)", sender.acc.Address.String(), hex.EncodeToString(tools.HexReverse(maxFeeOfTransaction.param.TxHash)))

				}
				// txSenChan <- sender
			}
			log.Infof("sender %s is done", sender.acc.Address.String())
		}(txSend[i], this.txLock, this.senders[i])
	}

	log.Infof("wg.Wait start")
	wg.Wait()
	log.Infof("wg.Wait finished")


	for k, v := range retryBridgeTransactions {
		sink := common.NewZeroCopySink(nil)
		v.Serialization(sink)
		this.db.PutBridgeTransactions(k, sink.Bytes())
	}
	return nil
}

func (this *PolyManager) Stop() {
	this.exitChan <- 1
	close(this.exitChan)
	log.Infof("poly chain manager exit.")
}

func (this *PolyManager) checkFee(checks []*poly_bridge_sdk.CheckFeeReq) ([]*poly_bridge_sdk.CheckFeeRsp, error) {
	return this.bridgeSdk.CheckFee(checks)
}

type EthSender struct {
	acc          accounts.Account
	keyStore     *tools.EthKeyStore
	cmap         map[string]chan *EthTxInfo
	result       chan bool
	locked       bool
	id           int
	nonceManager *tools.NonceManager
	ethClient    *ethclient.Client
	polySdk      *sdk.PolySdk
	config       *config.ServiceConfig
	contractAbi  *abi.ABI
	eccdInstance *eccd_abi.EthCrossChainData
}

func (this *EthSender) sendTxToEth(info *EthTxInfo) error {
	nonce := this.nonceManager.GetAddressNonce(this.acc.Address)
	origin := big.NewInt(0).Set(info.gasPrice)
	maxPrice := big.NewInt(0).Quo(big.NewInt(0).Mul(origin, big.NewInt(100)), big.NewInt(10))
RETRY:
	tx := types.NewTransaction(nonce, info.contractAddr, big.NewInt(0), info.gasLimit, info.gasPrice, info.txData)
	signedtx, err := this.keyStore.SignTransaction(tx, this.acc)
	if err != nil {
		this.nonceManager.ReturnNonce(this.acc.Address, nonce)
		return fmt.Errorf("commitDepositEventsWithHeader - sign raw tx error and return nonce %d: %v", nonce, err)
	}
	err = this.ethClient.SendTransaction(context.Background(), signedtx)
	if err != nil {
		/*
		 this.nonceManager.ReturnNonce(this.acc.Address, nonce)
		 return fmt.Errorf("commitDepositEventsWithHeader - send transaction error and return nonce %d: %v", nonce, err)
		*/
		log.Errorf("send transactions err: %v", err)
		os.Exit(1)
		//log.Fatal("send transaction error!")
	}
	hash := signedtx.Hash()

	err2 := this.waitTransactionConfirm(info.polyTxHash, hash)
	if err2 == nil {
		log.Infof("successful to relay tx to ethereum: (eth_hash: %s, nonce: %d, poly_hash: %s, eth_explorer: %s)",
			hash.String(), nonce, tools.HexStringReverse(info.polyTxHash), tools.GetExplorerUrl(this.keyStore.GetChainId())+hash.String())
	} else {
		log.Errorf("failed to relay tx to ethereum: (eth_hash: %s, nonce: %d, poly_hash: %s, eth_explorer: %s), err: %w",
			hash.String(), nonce, tools.HexStringReverse(info.polyTxHash), tools.GetExplorerUrl(this.keyStore.GetChainId())+hash.String(), err2)
		if info.gasPrice.Cmp(maxPrice) > 0 {
			log.Errorf("waitTransactionConfirm failed")
			os.Exit(1)
		}
		info.gasPrice = big.NewInt(0).Quo(big.NewInt(0).Mul(info.gasPrice, big.NewInt(11)), big.NewInt(10))
		if info.gasPrice.Cmp(maxPrice) >= 0 {
			info.gasPrice.Set(maxPrice)
		}
		goto RETRY
		//this.nonceManager.ReturnNonce(this.acc.Address, nonce)
	}
	if !this.locked {
		// this.result <- true
	} else {
		log.Errorf("account %s has unlocked! - ", this.acc.Address.String())
		this.locked = false
	}
	// result <- true
	return nil
}

func (this *EthSender) commitDepositEventsWithHeader(header *polytypes.Header, param *common2.ToMerkleValue, headerProof string, anchorHeader *polytypes.Header, polyTxHash string, rawAuditPath []byte) bool {
	var (
		sigs       []byte
		headerData []byte
	)
	if anchorHeader != nil && headerProof != "" {
		for _, sig := range anchorHeader.SigData {
			temp := make([]byte, len(sig))
			copy(temp, sig)
			newsig, _ := signature.ConvertToEthCompatible(temp)
			sigs = append(sigs, newsig...)
		}
	} else {
		for _, sig := range header.SigData {
			temp := make([]byte, len(sig))
			copy(temp, sig)
			newsig, _ := signature.ConvertToEthCompatible(temp)
			sigs = append(sigs, newsig...)
		}
	}
	fromTx := [32]byte{}
	copy(fromTx[:], param.TxHash[:32])
	res, _ := this.eccdInstance.CheckIfFromChainTxExist(nil, param.FromChainID, fromTx)
	if res {
		log.Infof("already relayed to eth: ( from chain id: %d, poly txhash: %s,  from txhash: %s)",
			param.FromChainID, hex.EncodeToString(tools.HexReverse(param.TxHash)), hex.EncodeToString(param.MakeTxParam.TxHash))
		return true
	}
	//log.Infof("poly proof with header, height: %d, key: %s, proof: %s", header.Height-1, string(key), proof.AuditPath)

	rawProof, _ := hex.DecodeString(headerProof)
	var rawAnchor []byte
	if anchorHeader != nil {
		rawAnchor = anchorHeader.GetMessage()
	}
	headerData = header.GetMessage()
	txData, err := this.contractAbi.Pack("verifyHeaderAndExecuteTx", rawAuditPath, headerData, rawProof, rawAnchor, sigs)
	if err != nil {
		log.Errorf("commitDepositEventsWithHeader - err:" + err.Error())
		return false
	}

	gasPrice, err := this.ethClient.SuggestGasPrice(context.Background())
	if err != nil {
		log.Errorf("commitDepositEventsWithHeader - get suggest sas price failed error: %s", err.Error())
		return false
	}
	contractaddr := ethcommon.HexToAddress(this.config.ETHConfig.ECCMContractAddress)
	callMsg := ethereum.CallMsg{
		From: this.acc.Address, To: &contractaddr, Gas: 0, GasPrice: gasPrice,
		Value: big.NewInt(0), Data: txData,
	}
	gasLimit, err := this.ethClient.EstimateGas(context.Background(), callMsg)
	if err != nil {
		log.Errorf("commitDepositEventsWithHeader - estimate gas limit error，from chain id: %d, poly txhash: %s,  from txhash: %s， error: %s",
			param.FromChainID, hex.EncodeToString(tools.HexReverse(param.TxHash)), hex.EncodeToString(param.MakeTxParam.TxHash), err.Error())
		return false
	}

	//k := this.getRouter()
	//c, ok := this.cmap[k]
	result := make(chan bool)
	c := &EthTxInfo{
		txData:       txData,
		contractAddr: contractaddr,
		gasPrice:     gasPrice,
		gasLimit:     gasLimit,
		polyTxHash:   polyTxHash,
	}
	//if !ok {
		//c = make(chan *EthTxInfo, ChanLen)
		//this.cmap[k] = c
		go func(v *EthTxInfo) {
			//for v := range c {
				log.Infof("start to send tx to ethereum: poly txhash: %s", tools.HexStringReverse(v.polyTxHash))
				if err = this.sendTxToEth(v); err != nil {
					log.Errorf("failed to send tx to ethereum: error: %v, polyhash: %s", err, tools.HexStringReverse(v.polyTxHash))
					//result <- true
				} else {
					log.Infof("success to send tx to ethereum: polyhash: %s", tools.HexStringReverse(v.polyTxHash))
				}
				result <- true
			//}
		}(c)
	//}
	//TODO: could be blocked
	
	select {
	case <-result:
		return true
	case <-time.After(time.Second * 300):
		log.Errorf("account %s has locked!", this.acc.Address.String())
		this.locked = true
		return false
	}
}

func (this *EthSender) commitHeader(header *polytypes.Header, pubkList []byte) bool {
	headerdata := header.GetMessage()
	var (
		txData []byte
		txErr  error
		sigs   []byte
	)
	gasPrice, err := this.ethClient.SuggestGasPrice(context.Background())
	if err != nil {
		log.Errorf("commitHeader - get suggest sas price failed error: %s", err.Error())
		return false
	}
	for _, sig := range header.SigData {
		temp := make([]byte, len(sig))
		copy(temp, sig)
		newsig, _ := signature.ConvertToEthCompatible(temp)
		sigs = append(sigs, newsig...)
	}

	txData, txErr = this.contractAbi.Pack("changeBookKeeper", headerdata, pubkList, sigs)
	if txErr != nil {
		log.Errorf("commitHeader - err:" + err.Error())
		return false
	}

	contractaddr := ethcommon.HexToAddress(this.config.ETHConfig.ECCMContractAddress)
	callMsg := ethereum.CallMsg{
		From: this.acc.Address, To: &contractaddr, Gas: 0, GasPrice: gasPrice,
		Value: big.NewInt(0), Data: txData,
	}

	gasLimit, err := this.ethClient.EstimateGas(context.Background(), callMsg)
	if err != nil {
		log.Errorf("commitHeader - estimate gas limit error: %s", err.Error())
		return false
	}

	nonce := this.nonceManager.GetAddressNonce(this.acc.Address)
	tx := types.NewTransaction(nonce, contractaddr, big.NewInt(0), gasLimit, gasPrice, txData)
	signedtx, err := this.keyStore.SignTransaction(tx, this.acc)
	if err != nil {
		log.Errorf("commitHeader - sign raw tx error: %s", err.Error())
		return false
	}
	if err = this.ethClient.SendTransaction(context.Background(), signedtx); err != nil {
		log.Errorf("commitHeader - send transaction error:%s\n", err.Error())
		return false
	}

	hash := header.Hash()
	txhash := signedtx.Hash()
	err2 := this.waitTransactionConfirm(fmt.Sprintf("header: %d", header.Height), txhash)
	if err2 == nil {
		log.Infof("successful to relay poly header to ethereum: (header_hash: %s, height: %d, eth_txhash: %s, nonce: %d, eth_explorer: %s)",
			hash.ToHexString(), header.Height, txhash.String(), nonce, tools.GetExplorerUrl(this.keyStore.GetChainId())+txhash.String())
	} else {
		log.Errorf("failed to relay poly header to ethereum: (header_hash: %s, height: %d, eth_txhash: %s, nonce: %d, eth_explorer: %s), err: %w",
			hash.ToHexString(), header.Height, txhash.String(), nonce, tools.GetExplorerUrl(this.keyStore.GetChainId())+txhash.String(), err2)
	}
	return true
}

func (this *EthSender) getRouter() string {
	return strconv.FormatInt(rand.Int63n(this.config.RoutineNum), 10)
}

func (this *EthSender) Balance() (*big.Int, error) {
	balance, err := this.ethClient.BalanceAt(context.Background(), this.acc.Address, nil)
	if err != nil {
		return nil, err
	}
	return balance, nil
}

// TODO: check the status of tx
func (this *EthSender) waitTransactionConfirm(polyTxHash string, hash ethcommon.Hash) error {
	count := 0
	for {
		if count > 60 {
			return fmt.Errorf("reach max try, polyTxHash: %s, ethhash: %s", tools.HexStringReverse(polyTxHash), hash.String())
		}
		time.Sleep(time.Second * 1)
		count++
		_, ispending, err := this.ethClient.TransactionByHash(context.Background(), hash)
		if err != nil {
			log.Warnf("TransactionByHash error retry, polyTxHash: %s, ethhash: %s, error: %w", tools.HexStringReverse(polyTxHash), hash.String(), err)
			continue
		}
		log.Debugf("( eth_transaction %s, poly_tx %s ) is pending: %v", hash.String(), tools.HexStringReverse(polyTxHash), ispending)
		if ispending {
			continue
		} else {
			_, err := this.ethClient.TransactionReceipt(context.Background(), hash)
			if err != nil {
				log.Warnf("TransactionReceipt error retry, polyTxHash: %s, ethhash: %s, error: %w", tools.HexStringReverse(polyTxHash), hash.String(), err)
				continue
			}
			return nil
			// return receipt.Status == types.ReceiptStatusSuccessful
		}
	}
}

type EthTxInfo struct {
	txData       []byte
	gasLimit     uint64
	gasPrice     *big.Int
	contractAddr ethcommon.Address
	polyTxHash   string
}
