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
package main

import (
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/urfave/cli"

	"github.com/ethereum/go-ethereum/ethclient"

	sdk "github.com/polynetwork/poly-go-sdk"
	"github.com/polynetwork/polygon-relayer/cosmos-sdk/codec"
	"github.com/polynetwork/polygon-relayer/manager"
	sdkp "github.com/polynetwork/polygon-relayer/poly_go_sdk"

	"github.com/polynetwork/polygon-relayer/cmd"
	"github.com/polynetwork/polygon-relayer/config"
	cosctx "github.com/polynetwork/polygon-relayer/cosmos-relayer/context"
	"github.com/polynetwork/polygon-relayer/cosmos-relayer/service"
	"github.com/polynetwork/polygon-relayer/db"
	"github.com/polynetwork/polygon-relayer/log"

	"github.com/polynetwork/polygon-relayer/global"

	rpcclient "github.com/christianxiao/tendermint/rpc/client"
)

var ConfigPath string
var LogDir string
var StartHeight uint64
var PolyStartHeight uint64
var StartForceHeight uint64

func setupApp() *cli.App {
	app := cli.NewApp()
	app.Usage = "Polygon relayer Service"
	app.Action = startServer
	app.Version = config.Version
	app.Copyright = "Copyright in 2019 The Ontology Authors"
	app.Flags = []cli.Flag{
		cmd.LogLevelFlag,
		cmd.ConfigPathFlag,
		cmd.EthStartFlag,
		cmd.EthStartForceFlag,
		cmd.PolyStartFlag,
		cmd.LogDir,
		cmd.TestLocalFlag,
		cmd.NofeemodeFlag,
	}
	app.Commands = []cli.Command{}
	app.Before = func(context *cli.Context) error {
		runtime.GOMAXPROCS(runtime.NumCPU())
		return nil
	}
	return app
}

func startServer(ctx *cli.Context) {
	// get all cmd flag
	logLevel := ctx.GlobalInt(cmd.GetFlagName(cmd.LogLevelFlag))

	ld := ctx.GlobalString(cmd.GetFlagName(cmd.LogDir))
	log.InitLog(logLevel, ld, log.Stdout)

	ConfigPath = ctx.GlobalString(cmd.GetFlagName(cmd.ConfigPathFlag))
	ethstart := ctx.GlobalUint64(cmd.GetFlagName(cmd.EthStartFlag))
	if ethstart > 0 {
		StartHeight = ethstart
	}

	StartForceHeight = 0
	ethstartforce := ctx.GlobalUint64(cmd.GetFlagName(cmd.EthStartForceFlag))
	if ethstartforce > 0 {
		StartForceHeight = ethstartforce
	}
	polyStart := ctx.GlobalUint64(cmd.GetFlagName(cmd.PolyStartFlag))
	if polyStart > 0 {
		PolyStartHeight = polyStart
	}

	testLocal := ctx.GlobalBool(cmd.GetFlagName(cmd.TestLocalFlag))
	nofeemode := ctx.GlobalBool(cmd.GetFlagName(cmd.NofeemodeFlag))

	// read config
	servConfig := config.NewServiceConfig(ConfigPath)
	if servConfig == nil {
		log.Errorf("startServer - create config failed!")
		return
	}

	global.ServiceConfig = servConfig

	if servConfig.ETHConfig.StartHeight > 0 {
		StartHeight = servConfig.ETHConfig.StartHeight
	}

	// create poly sdk
	polySdk := sdk.NewPolySdk()
	err := setUpPoly(polySdk, servConfig.PolyConfig.RestURL)
	if err != nil {
		log.Errorf("startServer - failed to setup poly sdk: %v", err)
		return
	}
	global.PolySdkp = sdkp.NewPolySdkp(polySdk, testLocal)

	// create ethereum sdk
	ethereumsdk, err := ethclient.Dial(servConfig.ETHConfig.RestURL)
	log.Infof("init eth client - start url : %s", servConfig.ETHConfig.RestURL)
	if err != nil {
		log.Errorf("startServer - cannot dial sync node, err: %s", err)
		return
	}
	global.Ethereumsdk = ethereumsdk

	var boltDB *db.BoltDB
	if servConfig.BoltDbPath == "" {
		boltDB, err = db.NewBoltDB("boltdb")
	} else {
		boltDB, err = db.NewBoltDB(servConfig.BoltDbPath)
	}
	if err != nil {
		log.Fatalf("db.NewWaitingDB error:%s", err)
		return
	}
	global.Db = boltDB

	// heimdall client
	tclient := rpcclient.NewHTTP(servConfig.TendermintConfig.CosmosRpcAddr, "/websocket")
	tclient.Start()
	global.Rpcclient = tclient

	// init heimdall service
	// all tools and info hold by context object.
	if err = cosctx.InitCtx(servConfig.TendermintConfig, boltDB, global.PolySdkp, tclient); err != nil {
		log.Fatalf("failed to init context: %v", err)
		panic(err)
	}

	service.StartListen()
	service.StartRelay()

	initPolyServer(servConfig, global.PolySdkp, ethereumsdk, boltDB, nofeemode)
	initETHServer(servConfig, global.PolySdkp, ethereumsdk, boltDB, cosctx.RCtx.CMCdc, tclient)
	waitToExit()
}

func setUpPoly(poly *sdk.PolySdk, RpcAddr string) error {
	poly.NewRpcClient().SetAddress(RpcAddr)
	hdr, err := poly.GetHeaderByHeight(0)
	if err != nil {
		return err
	}
	poly.SetChainId(hdr.ChainID)
	return nil
}

func waitToExit() {
	exit := make(chan bool)
	sc := make(chan os.Signal, 1)
	signal.Notify(sc, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	go func() {
		for sig := range sc {
			log.Infof("waitToExit - ETH relayer received exit signal:%v.", sig.String())
			close(exit)
			break
		}
	}()
	<-exit
}

func initETHServer(servConfig *config.ServiceConfig, polysdk *sdkp.PolySdk, ethereumsdk *ethclient.Client, boltDB *db.BoltDB, cdc *codec.Codec, tclient *rpcclient.HTTP) {
	mgr, err := manager.NewEthereumManager(servConfig, StartHeight, StartForceHeight, polysdk, ethereumsdk, boltDB, servConfig.TendermintConfig.CosmosRpcAddr, cdc, tclient)
	if err != nil {
		log.Error("initETHServer - eth service start err: %s", err.Error())
		return
	}

	// start save spanId => bor height map
	go mgr.TendermintClient.MonitorSpanLatestRoutine(servConfig.TendermintConfig.SpanInterval)
	go mgr.TendermintClient.MonitorSpanHisRoutine(uint64(servConfig.TendermintConfig.SpanStart))

	go mgr.MonitorChain()
	
	go mgr.MonitorDeposit()
	go mgr.CheckDeposit()
}

func initPolyServer(servConfig *config.ServiceConfig, polysdk *sdkp.PolySdk, ethereumsdk *ethclient.Client, boltDB *db.BoltDB, nofeemode bool) {
	mgr, err := manager.NewPolyManager(servConfig, uint32(PolyStartHeight), polysdk, ethereumsdk, boltDB, nofeemode)
	if err != nil {
		log.Error("initPolyServer - PolyServer service start failed: %v", err)
		return
	}
	go mgr.MonitorChain()
	go mgr.MonitorDeposit()
}

func main() {
	log.Infof("main - ETH relayer starting...")
	if err := setupApp().Run(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
