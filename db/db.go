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
package db

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
	"path"
	"sort"
	"strings"
	"sync"

	"github.com/boltdb/bolt"
	"github.com/polynetwork/polygon-relayer/tools"
)

const MAX_NUM = 1000

var (
	BKTCheck  = []byte("Check")
	BKTRetry  = []byte("Retry")
	BKTHeight = []byte("Height")

	BKTBridgeTransactions = []byte("Bridge Transactions")

	BKTSpan = []byte("Span") //bor block height => spanId, span data

	// tendermint
	PolyState       = []byte("poly")
	COSMOSState     = []byte("cosmos")
	CosmosReProve   = []byte("cosmos_reprove")
	PolyReProve     = []byte("poly_reprove")
	CosmosStatusKey = []byte("cosmos_status")
	PolyStatusKey   = []byte("poly_status")
)

type BoltDB struct {
	rwlock   *sync.RWMutex
	db       *bolt.DB
	filePath string
}

func NewBoltDB(filePath string) (*BoltDB, error) {
	err := os.MkdirAll(filePath, os.ModePerm)
	if err != nil {
		return nil, err
	}
	
	if !strings.Contains(filePath, ".bin") {
		filePath = path.Join(filePath, "bolt.bin")
	}
	w := new(BoltDB)
	db, err := bolt.Open(filePath, 0644, &bolt.Options{InitialMmapSize: 500000})
	if err != nil {
		return nil, err
	}
	w.db = db
	w.rwlock = new(sync.RWMutex)
	w.filePath = filePath

	if err = db.Update(func(btx *bolt.Tx) error {
		_, err := btx.CreateBucketIfNotExists(BKTCheck)
		if err != nil {
			return err
		}

		return nil
	}); err != nil {
		return nil, err
	}

	if err = db.Update(func(btx *bolt.Tx) error {
		_, err := btx.CreateBucketIfNotExists(BKTRetry)
		if err != nil {
			return err
		}

		return nil
	}); err != nil {
		return nil, err
	}

	if err = db.Update(func(btx *bolt.Tx) error {
		_, err := btx.CreateBucketIfNotExists(BKTHeight)
		if err != nil {
			return err
		}

		return nil
	}); err != nil {
		return nil, err
	}

	if err = db.Update(func(btx *bolt.Tx) error {
		_, err := btx.CreateBucketIfNotExists(BKTSpan)
		if err != nil {
			return err
		}

		return nil
	}); err != nil {
		return nil, err
	}

	// tendermint
	if err = db.Update(func(btx *bolt.Tx) error {
		_, err := btx.CreateBucketIfNotExists(PolyState)
		if err != nil {
			return err
		}

		return nil
	}); err != nil {
		return nil, err
	}

	if err = db.Update(func(btx *bolt.Tx) error {
		_, err := btx.CreateBucketIfNotExists(COSMOSState)
		if err != nil {
			return err
		}

		return nil
	}); err != nil {
		return nil, err
	}

	if err = db.Update(func(btx *bolt.Tx) error {
		_, err := btx.CreateBucketIfNotExists(CosmosReProve)
		if err != nil {
			return err
		}

		return nil
	}); err != nil {
		return nil, err
	}

	if err = db.Update(func(btx *bolt.Tx) error {
		_, err := btx.CreateBucketIfNotExists(PolyReProve)
		if err != nil {
			return err
		}

		return nil
	}); err != nil {
		return nil, err
	}

	if err = db.Update(func(btx *bolt.Tx) error {
		_, err := btx.CreateBucketIfNotExists(CosmosStatusKey)
		if err != nil {
			return err
		}

		return nil
	}); err != nil {
		return nil, err
	}

	if err = db.Update(func(btx *bolt.Tx) error {
		_, err := btx.CreateBucketIfNotExists(PolyStatusKey)
		if err != nil {
			return err
		}

		return nil
	}); err != nil {
		return nil, err
	}

	if err = db.Update(func(btx *bolt.Tx) error {
		_, err := btx.CreateBucketIfNotExists(BKTBridgeTransactions)
		if err != nil {
			return err
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return w, nil
}

func (w *BoltDB) Put(name []byte, k []byte, v []byte) error {
	w.rwlock.Lock()
	defer w.rwlock.Unlock()

	return w.db.Update(func(btx *bolt.Tx) error {
		bucket := btx.Bucket(name)
		err := bucket.Put(k, v)
		if err != nil {
			return err
		}
		return nil
	})
}

func (w *BoltDB) PutUint64(name []byte, k uint64, v []byte) error {
	w.rwlock.Lock()
	defer w.rwlock.Unlock()

	return w.db.Update(func(btx *bolt.Tx) error {
		bucket := btx.Bucket(name)
		kbs := tools.Uint64ToBigEndian(k)
		err2 := bucket.Put(kbs, v)
		if err2 != nil {
			return err2
		}
		return nil
	})
}

func (w *BoltDB) GetUint64(name []byte, k uint64) ([]byte, error) {
	w.rwlock.RLock()
	defer w.rwlock.RUnlock()

	var val []byte
	_ = w.db.View(func(btx *bolt.Tx) error {
		bucket := btx.Bucket(name)
		kbs := tools.Uint64ToBigEndian(k)
		val = bucket.Get(kbs)
		if val == nil {
			return nil
		}
		return nil
	})
	return val, nil
}

type KeyValue struct {
	K uint64
	V []byte
}

func (w *BoltDB) Get(name []byte, k []byte) []byte {
	w.rwlock.RLock()
	defer w.rwlock.RUnlock()

	var v []byte
	_ = w.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(name)
		raw := bkt.Get(k)
		if len(raw) == 0 {
			v = nil
			return nil
		}
		//h = binary.LittleEndian.Uint32(raw)
		v = raw
		return nil
	})
	return v
}

func (w *BoltDB) GetAllUint64(name []byte) ([]*KeyValue, error) {
	w.rwlock.Lock()
	defer w.rwlock.Unlock()

	checkMap := make([]*KeyValue, 0)
	err := w.db.Update(func(tx *bolt.Tx) error {
		bw := tx.Bucket(name)
		bw.ForEach(func(k, v []byte) error {
			_k := make([]byte, len(k))
			_v := make([]byte, len(v))
			copy(_k, k)
			copy(_v, v)
			checkMap = append(checkMap, &KeyValue{
				K: tools.BigEndianToUint64(_k),
				V: _v,
			})

			return nil
		})
		return nil
	})
	if err != nil {
		return nil, err
	}

	sort.Slice(checkMap, func(i, j int) bool {
		return checkMap[i].K > checkMap[j].K
	})
	return checkMap, nil
}

func (w *BoltDB) PutCheck(txHash string, v []byte) error {
	w.rwlock.Lock()
	defer w.rwlock.Unlock()

	k, err := hex.DecodeString(txHash)
	if err != nil {
		return err
	}
	return w.db.Update(func(btx *bolt.Tx) error {
		bucket := btx.Bucket(BKTCheck)
		err := bucket.Put(k, v)
		if err != nil {
			return err
		}

		return nil
	})
}

func (w *BoltDB) DeleteCheck(txHash string) error {
	w.rwlock.Lock()
	defer w.rwlock.Unlock()

	k, err := hex.DecodeString(txHash)
	if err != nil {
		return err
	}
	return w.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(BKTCheck)
		err := bucket.Delete(k)
		if err != nil {
			return err
		}
		return nil
	})
}

func (w *BoltDB) PutRetry(k []byte) error {
	w.rwlock.Lock()
	defer w.rwlock.Unlock()

	return w.db.Update(func(btx *bolt.Tx) error {
		bucket := btx.Bucket(BKTRetry)
		err := bucket.Put(k, []byte{0x00})
		if err != nil {
			return err
		}

		return nil
	})
}

func (w *BoltDB) DeleteRetry(k []byte) error {
	w.rwlock.Lock()
	defer w.rwlock.Unlock()

	return w.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(BKTRetry)
		err := bucket.Delete(k)
		if err != nil {
			return err
		}
		return nil
	})
}

func (w *BoltDB) GetAllCheck() (map[string][]byte, error) {
	w.rwlock.Lock()
	defer w.rwlock.Unlock()

	checkMap := make(map[string][]byte)
	err := w.db.Update(func(tx *bolt.Tx) error {
		bw := tx.Bucket(BKTCheck)
		bw.ForEach(func(k, v []byte) error {
			_k := make([]byte, len(k))
			_v := make([]byte, len(v))
			copy(_k, k)
			copy(_v, v)
			checkMap[hex.EncodeToString(_k)] = _v
			if len(checkMap) >= MAX_NUM {
				return fmt.Errorf("max num")
			}
			return nil
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return checkMap, nil
}

func (w *BoltDB) GetAllRetry() ([][]byte, error) {
	w.rwlock.Lock()
	defer w.rwlock.Unlock()

	retryList := make([][]byte, 0)
	err := w.db.Update(func(tx *bolt.Tx) error {
		bw := tx.Bucket(BKTRetry)
		bw.ForEach(func(k, _ []byte) error {
			_k := make([]byte, len(k))
			copy(_k, k)
			retryList = append(retryList, _k)
			if len(retryList) >= MAX_NUM {
				return fmt.Errorf("max num")
			}
			return nil
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return retryList, nil
}

func (w *BoltDB) UpdatePolyHeight(h uint32) error {
	w.rwlock.Lock()
	defer w.rwlock.Unlock()

	raw := make([]byte, 4)
	binary.LittleEndian.PutUint32(raw, h)

	return w.db.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(BKTHeight)
		return bkt.Put([]byte("poly_height"), raw)
	})
}

func (w *BoltDB) GetPolyHeight() uint32 {
	w.rwlock.RLock()
	defer w.rwlock.RUnlock()

	var h uint32
	_ = w.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket(BKTHeight)
		raw := bkt.Get([]byte("poly_height"))
		if len(raw) == 0 {
			h = 0
			return nil
		}
		h = binary.LittleEndian.Uint32(raw)
		return nil
	})
	return h
}

func (w *BoltDB) Close() {
	w.rwlock.Lock()
	w.db.Close()
	w.rwlock.Unlock()
}


func (db *BoltDB) SetCosmosHeight(height int64) error {
	db.rwlock.RLock()
	defer db.rwlock.RUnlock()

	val := make([]byte, 8)
	binary.LittleEndian.PutUint64(val, uint64(height))
	return db.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(COSMOSState)
		err := bucket.Put(COSMOSState, val)
		if err != nil {
			return err
		}
		return nil
	})
}

func (db *BoltDB) GetCosmosHeight() int64 {
	db.rwlock.RLock()
	defer db.rwlock.RUnlock()

	var height uint64
	_ = db.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(COSMOSState)
		val := bucket.Get(COSMOSState)
		if val == nil {
			height = 0
			return nil
		}
		height = binary.LittleEndian.Uint64(val)
		return nil
	})

	return int64(height)
}

func (w *BoltDB) PutBridgeTransactions(txHash string, v []byte) error {
	w.rwlock.Lock()
	defer w.rwlock.Unlock()
	k := []byte(txHash)
	return w.db.Update(func(btx *bolt.Tx) error {
		bucket := btx.Bucket(BKTBridgeTransactions)
		err := bucket.Put(k, v)
		if err != nil {
			return err
		}

		return nil
	})
}

func (w *BoltDB) DeleteBridgeTransactions(txHash string) error {
	w.rwlock.Lock()
	defer w.rwlock.Unlock()
	k := []byte(txHash)
	return w.db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(BKTBridgeTransactions)
		err := bucket.Delete(k)
		if err != nil {
			return err
		}
		return nil
	})
}

func (w *BoltDB) GetAllBridgeTransactions() (map[string][]byte, error) {
	w.rwlock.Lock()
	defer w.rwlock.Unlock()

	checkMap := make(map[string][]byte)
	err := w.db.Update(func(tx *bolt.Tx) error {
		bw := tx.Bucket(BKTBridgeTransactions)
		bw.ForEach(func(k, v []byte) error {
			_k := make([]byte, len(k))
			_v := make([]byte, len(v))
			copy(_k, k)
			copy(_v, v)
			checkMap[string(_k)] = _v
			if len(checkMap) >= MAX_NUM {
				return fmt.Errorf("max num")
			}
			return nil
		})
		return nil
	})
	if err != nil {
		return nil, err
	}
	return checkMap, nil
}
