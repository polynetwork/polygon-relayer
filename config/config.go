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
package config

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"github.com/polynetwork/polygon-relayer/log"
)

const (
	ETH_MONITOR_INTERVAL  = 1 * time.Second
	POLY_MONITOR_INTERVAL = 1 * time.Second

	ETH_USEFUL_BLOCK_NUM     = 128
	ETH_PROOF_USERFUL_BLOCK  = 12
	ONT_USEFUL_BLOCK_NUM     = 1
	DEFAULT_CONFIG_FILE_NAME = "./config.json"
	Version                  = "1.0"

	DEFAULT_LOG_LEVEL = log.InfoLog
)

type ServiceConfig struct {
	PolyConfig      *PolyConfig
	ETHConfig       *ETHConfig
	TendermintConfig	*TendermintConfig
	BoltDbPath      string
	RoutineNum      int64
	TargetContracts []map[string]map[string][]uint64
	BridgeUrl       [][]string
}

type PolyConfig struct {
	RestURL                 string
	EntranceContractAddress string
	WalletFile              string
	WalletPwd               string
}

type ETHConfig struct {
	SideChainId         uint64
	StartHeight 		uint64
	RestURL             string
	ECCMContractAddress string
	ECCDContractAddress string
	KeyStorePath        string
	KeyStorePwdSet      map[string]string
	BlockConfig         uint64
	HeadersPerBatch     int
	MonitorInterval     uint64
}

type TendermintConfig struct {
	SpanInterval uint64
	SpanStart uint64

	CosmosRpcAddr        string
	CosmosStartHeight    int64  
	HeadersPerBatch      int
	CosmosListenInterval int    

	PolyRpcAddr        string 
	PolyWallet         string 
	PolyWalletPwd      string   

	SideChainId    uint64 

	ConfirmTimeout int
}

func ReadFile(fileName string) ([]byte, error) {
	file, err := os.OpenFile(fileName, os.O_RDONLY, 0666)
	if err != nil {
		return nil, fmt.Errorf("ReadFile: open file %s error %s", fileName, err)
	}
	defer func() {
		err := file.Close()
		if err != nil {
			log.Errorf("ReadFile: File %s close error %s", fileName, err)
		}
	}()
	data, err := ioutil.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("ReadFile: ioutil.ReadAll %s error %s", fileName, err)
	}
	return data, nil
}

func NewServiceConfig(configFilePath string) *ServiceConfig {
	fileContent, err := ReadFile(configFilePath)
	if err != nil {
		log.Errorf("NewServiceConfig: failed, err: %s", err)
		return nil
	}
	servConfig := &ServiceConfig{}
	err = json.Unmarshal(fileContent, servConfig)
	if err != nil {
		log.Errorf("NewServiceConfig: failed, err: %s", err)
		return nil
	}

	servConfig.TendermintConfig.PolyRpcAddr = servConfig.PolyConfig.RestURL
	servConfig.TendermintConfig.PolyWallet = servConfig.PolyConfig.WalletFile
	servConfig.TendermintConfig.PolyWalletPwd = servConfig.PolyConfig.WalletPwd

	for k, v := range servConfig.ETHConfig.KeyStorePwdSet {
		delete(servConfig.ETHConfig.KeyStorePwdSet, k)
		servConfig.ETHConfig.KeyStorePwdSet[strings.ToLower(k)] = v
	}

	return servConfig
}
