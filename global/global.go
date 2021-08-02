package global

import (
	"github.com/polynetwork/polygon-relayer/config"

	sdkp "github.com/polynetwork/polygon-relayer/poly_go_sdk"
	"github.com/ethereum/go-ethereum/ethclient"
	db "github.com/polynetwork/polygon-relayer/db"

	rpcclient "github.com/christianxiao/tendermint/rpc/client"
)

var ServiceConfig *config.ServiceConfig

var PolySdkp  *sdkp.PolySdk
var Ethereumsdk *ethclient.Client

var Db *db.BoltDB

var Rpcclient *rpcclient.HTTP
