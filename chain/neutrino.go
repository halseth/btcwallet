package chain

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/btcsuite/btcd/chaincfg"
	"github.com/btcsuite/btcd/chaincfg/chainhash"
	"github.com/btcsuite/btcd/rpcclient"
	"github.com/btcsuite/btcd/txscript"
	"github.com/btcsuite/btcd/wire"
	"github.com/btcsuite/btcutil"
	"github.com/btcsuite/btcutil/gcs"
	"github.com/btcsuite/btcutil/gcs/builder"
	"github.com/btcsuite/btcwallet/waddrmgr"
	"github.com/btcsuite/btcwallet/wtxmgr"
	"github.com/lightninglabs/neutrino"
)

// NeutrinoClient is an implementation of the btcwalet chain.Interface interface.
type NeutrinoClient struct {
	CS *neutrino.ChainService

	chainParams *chaincfg.Params

	// We currently support one rescan/notifiction goroutine per client
	rescan *neutrino.Rescan

	enqueueNotification chan interface{}
	dequeueNotification chan interface{}
	startTime           time.Time
	lastProgressSent    bool
	currentBlock        chan *waddrmgr.BlockStamp

	quit       chan struct{}
	rescanQuit chan struct{}
	rescanErr  <-chan error
	wg         sync.WaitGroup
	started    bool
	scanning   bool
	finished   bool
	isRescan   bool

	clientMtx sync.Mutex
}

// NewNeutrinoClient creates a new NeutrinoClient struct with a backing
// ChainService.
func NewNeutrinoClient(chainParams *chaincfg.Params,
	chainService *neutrino.ChainService) *NeutrinoClient {

	return &NeutrinoClient{
		CS:          chainService,
		chainParams: chainParams,
	}
}

// BackEnd returns the name of the driver.
func (s *NeutrinoClient) BackEnd() string {
	return "neutrino"
}

// Start replicates the RPC client's Start method.
func (s *NeutrinoClient) Start() error {
	s.CS.Start()
	s.clientMtx.Lock()
	defer s.clientMtx.Unlock()
	if !s.started {
		s.enqueueNotification = make(chan interface{})
		s.dequeueNotification = make(chan interface{})
		s.currentBlock = make(chan *waddrmgr.BlockStamp)
		s.quit = make(chan struct{})
		s.started = true
		s.wg.Add(1)
		go func() {
			select {
			case s.enqueueNotification <- ClientConnected{}:
			case <-s.quit:
			}
		}()
		go s.notificationHandler()
	}
	return nil
}

// Stop replicates the RPC client's Stop method.
func (s *NeutrinoClient) Stop() {
	s.clientMtx.Lock()
	defer s.clientMtx.Unlock()
	if !s.started {
		return
	}
	close(s.quit)
	s.started = false
}

// WaitForShutdown replicates the RPC client's WaitForShutdown method.
func (s *NeutrinoClient) WaitForShutdown() {
	s.wg.Wait()
}

// GetBlock replicates the RPC client's GetBlock command.
func (s *NeutrinoClient) GetBlock(hash *chainhash.Hash) (*wire.MsgBlock, error) {
	// TODO(roasbeef): add a block cache?
	//  * which evication strategy? depends on use case
	//  Should the block cache be INSIDE neutrino instead of in btcwallet?
	block, err := s.CS.GetBlock(*hash)
	if err != nil {
		return nil, err
	}
	return block.MsgBlock(), nil
}

// GetBlockHeight gets the height of a block by its hash. It serves as a
// replacement for the use of GetBlockVerboseTxAsync for the wallet package
// since we can't actually return a FutureGetBlockVerboseResult because the
// underlying type is private to rpcclient.
func (s *NeutrinoClient) GetBlockHeight(hash *chainhash.Hash) (int32, error) {
	_, height, err := s.CS.BlockHeaders.FetchHeader(hash)
	if err != nil {
		return 0, err
	}
	return int32(height), nil
}

// GetBestBlock replicates the RPC client's GetBestBlock command.
func (s *NeutrinoClient) GetBestBlock() (*chainhash.Hash, int32, error) {
	chainTip, err := s.CS.BestSnapshot()
	if err != nil {
		return nil, 0, err
	}

	return &chainTip.Hash, chainTip.Height, nil
}

// BlockStamp returns the latest block notified by the client, or an error
// if the client has been shut down.
func (s *NeutrinoClient) BlockStamp() (*waddrmgr.BlockStamp, error) {
	select {
	case bs := <-s.currentBlock:
		return bs, nil
	case <-s.quit:
		return nil, errors.New("disconnected")
	}
}

// GetBlockHash returns the block hash for the given height, or an error if the
// client has been shut down or the hash at the block height doesn't exist or
// is unknown.
func (s *NeutrinoClient) GetBlockHash(height int64) (*chainhash.Hash, error) {
	header, err := s.CS.BlockHeaders.FetchHeaderByHeight(uint32(height))
	if err != nil {
		return nil, err
	}
	hash := header.BlockHash()
	return &hash, nil
}

// GetBlockHeader returns the block header for the given block hash, or an error
// if the client has been shut down or the hash doesn't exist or is unknown.
func (s *NeutrinoClient) GetBlockHeader(
	blockHash *chainhash.Hash) (*wire.BlockHeader, error) {
	header, _, err := s.CS.BlockHeaders.FetchHeader(blockHash)
	return header, err
}

// SendRawTransaction replicates the RPC client's SendRawTransaction command.
func (s *NeutrinoClient) SendRawTransaction(tx *wire.MsgTx, allowHighFees bool) (
	*chainhash.Hash, error) {
	err := s.CS.SendTransaction(tx)
	if err != nil {
		return nil, err
	}
	hash := tx.TxHash()
	return &hash, nil
}

// FilterBlocks scans the blocks contained in the FilterBlocksRequest for any
// addresses of interest. For each requested block, the corresponding compact
// filter will first be checked for matches, skipping those that do not report
// anything. If the filter returns a postive match, the full block will be
// fetched and filtered. This method returns a FilterBlocksReponse for the first
// block containing a matching address. If no matches are found in the range of
// blocks requested, the returned response will be nil.
func (s *NeutrinoClient) FilterBlocks(
	req *FilterBlocksRequest) (*FilterBlocksResponse, error) {

	blockFilterer := NewBlockFilterer(s.chainParams, req)

	// Construct the watchlist using the addresses and outpoints contained
	// in the filter blocks request.
	watchList, err := buildFilterBlocksWatchList(req)
	if err != nil {
		return nil, err
	}

	// Iterate over the requested blocks, fetching the compact filter for
	// each one, and matching it against the watchlist generated above. If
	// the filter returns a positive match, the full block is then requested
	// and scanned for addresses using the block filterer.
	for i, blk := range req.Blocks {
		filter, err := s.pollCFilter(&blk.Hash)
		if err != nil {
			return nil, err
		}

		// Skip any empty filters.
		if filter == nil || filter.N() == 0 {
			continue
		}

		key := builder.DeriveKey(&blk.Hash)
		matched, err := filter.MatchAny(key, watchList)
		if err != nil {
			return nil, err
		} else if !matched {
			continue
		}

		log.Infof("Fetching block height=%d hash=%v",
			blk.Height, blk.Hash)

		// TODO(conner): can optimize bandwidth by only fetching
		// stripped blocks
		rawBlock, err := s.GetBlock(&blk.Hash)
		if err != nil {
			return nil, err
		}

		if !blockFilterer.FilterBlock(rawBlock) {
			continue
		}

		// If any external or internal addresses were detected in this
		// block, we return them to the caller so that the rescan
		// windows can widened with subsequent addresses. The
		// `BatchIndex` is returned so that the caller can compute the
		// *next* block from which to begin again.
		resp := &FilterBlocksResponse{
			BatchIndex:         uint32(i),
			BlockMeta:          blk,
			FoundExternalAddrs: blockFilterer.FoundExternal,
			FoundInternalAddrs: blockFilterer.FoundInternal,
			FoundOutPoints:     blockFilterer.FoundOutPoints,
			RelevantTxns:       blockFilterer.RelevantTxns,
		}

		return resp, nil
	}

	// No addresses were found for this range.
	return nil, nil
}

// buildFilterBlocksWatchList constructs a watchlist used for matching against a
// cfilter from a FilterBlocksRequest. The watchlist will be populated with all
// external addresses, internal addresses, and outpoints contained in the
// request.
func buildFilterBlocksWatchList(req *FilterBlocksRequest) ([][]byte, error) {
	// Construct a watch list containing the script addresses of all
	// internal and external addresses that were requested, in addition to
	// the set of outpoints currently being watched.
	watchListSize := len(req.ExternalAddrs) +
		len(req.InternalAddrs) +
		len(req.WatchedOutPoints)

	watchList := make([][]byte, 0, watchListSize)

	for _, addr := range req.ExternalAddrs {
		p2shAddr, err := txscript.PayToAddrScript(addr)
		if err != nil {
			return nil, err
		}

		watchList = append(watchList, p2shAddr)
	}

	for _, addr := range req.InternalAddrs {
		p2shAddr, err := txscript.PayToAddrScript(addr)
		if err != nil {
			return nil, err
		}

		watchList = append(watchList, p2shAddr)
	}

	for _, addr := range req.WatchedOutPoints {
		addr, err := txscript.PayToAddrScript(addr)
		if err != nil {
			return nil, err
		}

		watchList = append(watchList, addr)
	}

	return watchList, nil
}

// pollCFilter attempts to fetch a CFilter from the neutrino client. This is
// used to get around the fact that the filter headers may lag behind the
// highest known block header.
func (s *NeutrinoClient) pollCFilter(hash *chainhash.Hash) (*gcs.Filter, error) {
	var (
		filter *gcs.Filter
		err    error
		count  int
	)

	const maxFilterRetries = 50
	for count < maxFilterRetries {
		if count > 0 {
			time.Sleep(100 * time.Millisecond)
		}

		filter, err = s.CS.GetCFilter(*hash, wire.GCSFilterRegular)
		if err != nil {
			count++
			continue
		}

		return filter, nil
	}

	return nil, err
}

// Rescan replicates the RPC client's Rescan command.
func (s *NeutrinoClient) Rescan(startHash *chainhash.Hash, addrs []btcutil.Address,
	outPoints map[wire.OutPoint]btcutil.Address) error {

	s.clientMtx.Lock()
	defer s.clientMtx.Unlock()
	if !s.started {
		return fmt.Errorf("can't do a rescan when the chain client " +
			"is not started")
	}

	var inputsToWatch []neutrino.InputWithScript
	for op, addr := range outPoints {
		addrScript, err := txscript.PayToAddrScript(addr)
		if err != nil {
		}

		inputsToWatch = append(inputsToWatch, neutrino.InputWithScript{
			OutPoint: op,
			PkScript: addrScript,
		})
	}

	// If we already have a rescan running, we can add the set of addresses
	// and inputs to it.
	if s.scanning {
		height, err := s.CS.BlockHeaders.HeightFromHash(startHash)
		if err != nil {
			return fmt.Errorf("couldn't get height for block "+
				"hash %v", startHash)
		}

		return s.rescan.Update(
			neutrino.Rewind(height),
			neutrino.AddAddrs(addrs...),
			neutrino.AddInputs(inputsToWatch...),
		)
	}

	s.rescanQuit = make(chan struct{})
	s.scanning = true
	s.finished = false
	s.lastProgressSent = false
	s.isRescan = true

	header, height, err := s.CS.BlockHeaders.ChainTip()
	if err != nil {
		return fmt.Errorf("Can't get chain service's best block: %s",
			err)
	}

	// If the wallet is already fully caught up, or the rescan has started
	// with state that indicates a "fresh" wallet, we'll send a
	// notification indicating the rescan has "finished".
	if header.BlockHash() == *startHash {
		s.finished = true
		select {
		case s.enqueueNotification <- &RescanFinished{
			Hash:   startHash,
			Height: int32(height),
			Time:   header.Timestamp,
		}:
		case <-s.quit:
			return nil
		case <-s.rescanQuit:
			return nil
		}
	}

	newRescan := s.CS.NewRescan(
		neutrino.NotificationHandlers(rpcclient.NotificationHandlers{
			OnBlockConnected:         s.onBlockConnected,
			OnFilteredBlockConnected: s.onFilteredBlockConnected,
			OnBlockDisconnected:      s.onBlockDisconnected,
		}),
		neutrino.StartBlock(&waddrmgr.BlockStamp{Hash: *startHash}),
		neutrino.StartTime(s.startTime),
		neutrino.QuitChan(s.rescanQuit),
		neutrino.WatchAddrs(addrs...),
		neutrino.WatchInputs(inputsToWatch...),
	)
	s.rescan = newRescan
	s.rescanErr = s.rescan.Start()

	return nil
}

// NotifyBlocks replicates the RPC client's NotifyBlocks command.
func (s *NeutrinoClient) NotifyBlocks() error {
	s.clientMtx.Lock()
	// If we're scanning, we're already notifying on blocks. Otherwise,
	// start a rescan without watching any addresses.
	if !s.scanning {
		s.clientMtx.Unlock()
		return s.NotifyReceived([]btcutil.Address{})
	}
	s.clientMtx.Unlock()
	return nil
}

// NotifyReceived replicates the RPC client's NotifyReceived command.
func (s *NeutrinoClient) NotifyReceived(addrs []btcutil.Address) error {
	s.clientMtx.Lock()

	// If we have a rescan running, we just need to add the appropriate
	// addresses to the watch list.
	if s.scanning {
		s.clientMtx.Unlock()
		return s.rescan.Update(neutrino.AddAddrs(addrs...))
	}

	s.rescanQuit = make(chan struct{})
	s.scanning = true

	// Don't need RescanFinished or RescanProgress notifications.
	s.finished = true
	s.lastProgressSent = true

	// Rescan with just the specified addresses.
	newRescan := s.CS.NewRescan(
		neutrino.NotificationHandlers(rpcclient.NotificationHandlers{
			OnBlockConnected:         s.onBlockConnected,
			OnFilteredBlockConnected: s.onFilteredBlockConnected,
			OnBlockDisconnected:      s.onBlockDisconnected,
		}),
		neutrino.StartTime(s.startTime),
		neutrino.QuitChan(s.rescanQuit),
		neutrino.WatchAddrs(addrs...),
	)
	s.rescan = newRescan
	s.rescanErr = s.rescan.Start()
	s.clientMtx.Unlock()
	return nil
}

// Notifications replicates the RPC client's Notifications method.
func (s *NeutrinoClient) Notifications() <-chan interface{} {
	return s.dequeueNotification
}

// SetStartTime is a non-interface method to set the birthday of the wallet
// using this object. Since only a single rescan at a time is currently
// supported, only one birthday needs to be set. This does not fully restart a
// running rescan, so should not be used to update a rescan while it is running.
// TODO: When factoring out to multiple rescans per Neutrino client, add a
// birthday per client.
func (s *NeutrinoClient) SetStartTime(startTime time.Time) {
	s.clientMtx.Lock()
	defer s.clientMtx.Unlock()

	s.startTime = startTime
}

// onFilteredBlockConnected sends appropriate notifications to the notification
// channel.
func (s *NeutrinoClient) onFilteredBlockConnected(height int32,
	header *wire.BlockHeader, relevantTxs []*btcutil.Tx) {
	ntfn := FilteredBlockConnected{
		Block: &wtxmgr.BlockMeta{
			Block: wtxmgr.Block{
				Hash:   header.BlockHash(),
				Height: height,
			},
			Time: header.Timestamp,
		},
	}
	for _, tx := range relevantTxs {
		rec, err := wtxmgr.NewTxRecordFromMsgTx(tx.MsgTx(),
			header.Timestamp)
		if err != nil {
			log.Errorf("Cannot create transaction record for "+
				"relevant tx: %s", err)
			// TODO(aakselrod): Return?
			continue
		}
		ntfn.RelevantTxs = append(ntfn.RelevantTxs, rec)
	}
	select {
	case s.enqueueNotification <- ntfn:
	case <-s.quit:
		return
	case <-s.rescanQuit:
		return
	}

	// Handle RescanFinished notification if required.
	bs, err := s.CS.BestSnapshot()
	if err != nil {
		log.Errorf("Can't get chain service's best block: %s", err)
		return
	}

	if bs.Hash == header.BlockHash() {
		// Only send the RescanFinished notification once.
		s.clientMtx.Lock()
		if s.finished {
			s.clientMtx.Unlock()
			return
		}
		// Only send the RescanFinished notification once the
		// underlying chain service sees itself as current.
		current := s.CS.IsCurrent() && s.lastProgressSent
		if current {
			s.finished = true
		}
		s.clientMtx.Unlock()
		if current {
			select {
			case s.enqueueNotification <- &RescanFinished{
				Hash:   &bs.Hash,
				Height: bs.Height,
				Time:   header.Timestamp,
			}:
			case <-s.quit:
				return
			case <-s.rescanQuit:
				return
			}
		}
	}
}

// onBlockDisconnected sends appropriate notifications to the notification
// channel.
func (s *NeutrinoClient) onBlockDisconnected(hash *chainhash.Hash, height int32,
	t time.Time) {
	select {
	case s.enqueueNotification <- BlockDisconnected{
		Block: wtxmgr.Block{
			Hash:   *hash,
			Height: height,
		},
		Time: t,
	}:
	case <-s.quit:
	case <-s.rescanQuit:
	}
}

func (s *NeutrinoClient) onBlockConnected(hash *chainhash.Hash, height int32,
	time time.Time) {
	// TODO: Move this closure out and parameterize it? Is it useful
	// outside here?
	sendRescanProgress := func() {
		select {
		case s.enqueueNotification <- &RescanProgress{
			Hash:   hash,
			Height: height,
			Time:   time,
		}:
		case <-s.quit:
		case <-s.rescanQuit:
		}
	}
	// Only send BlockConnected notification if we're processing blocks
	// before the birthday. Otherwise, we can just update using
	// RescanProgress notifications.
	if time.Before(s.startTime) {
		// Send a RescanProgress notification every 10K blocks.
		if height%10000 == 0 {
			s.clientMtx.Lock()
			shouldSend := s.isRescan && !s.finished
			s.clientMtx.Unlock()
			if shouldSend {
				sendRescanProgress()
			}
		}
	} else {
		// Send a RescanProgress notification if we're just going over
		// the boundary between pre-birthday and post-birthday blocks,
		// and note that we've sent it.
		s.clientMtx.Lock()
		if !s.lastProgressSent {
			shouldSend := s.isRescan && !s.finished
			if shouldSend {
				s.clientMtx.Unlock()
				sendRescanProgress()
				s.clientMtx.Lock()
				s.lastProgressSent = true
			}
		}
		s.clientMtx.Unlock()
		select {
		case s.enqueueNotification <- BlockConnected{
			Block: wtxmgr.Block{
				Hash:   *hash,
				Height: height,
			},
			Time: time,
		}:
		case <-s.quit:
		case <-s.rescanQuit:
		}
	}
}

// notificationHandler queues and dequeues notifications. There are currently
// no bounds on the queue, so the dequeue channel should be read continually to
// avoid running out of memory.
func (s *NeutrinoClient) notificationHandler() {
	hash, height, err := s.GetBestBlock()
	if err != nil {
		log.Errorf("Failed to get best block from chain service: %s",
			err)
		s.Stop()
		s.wg.Done()
		return
	}

	bs := &waddrmgr.BlockStamp{Hash: *hash, Height: height}

	// TODO: Rather than leaving this as an unbounded queue for all types of
	// notifications, try dropping ones where a later enqueued notification
	// can fully invalidate one waiting to be processed.  For example,
	// blockconnected notifications for greater block heights can remove the
	// need to process earlier blockconnected notifications still waiting
	// here.

	var notifications []interface{}
	enqueue := s.enqueueNotification
	var dequeue chan interface{}
	var next interface{}
out:
	for {
		s.clientMtx.Lock()
		rescanErr := s.rescanErr
		s.clientMtx.Unlock()
		select {
		case n, ok := <-enqueue:
			if !ok {
				// If no notifications are queued for handling,
				// the queue is finished.
				if len(notifications) == 0 {
					break out
				}
				// nil channel so no more reads can occur.
				enqueue = nil
				continue
			}
			if len(notifications) == 0 {
				next = n
				dequeue = s.dequeueNotification
			}
			notifications = append(notifications, n)

		case dequeue <- next:
			if n, ok := next.(BlockConnected); ok {
				bs = &waddrmgr.BlockStamp{
					Height: n.Height,
					Hash:   n.Hash,
				}
			}

			notifications[0] = nil
			notifications = notifications[1:]
			if len(notifications) != 0 {
				next = notifications[0]
			} else {
				// If no more notifications can be enqueued, the
				// queue is finished.
				if enqueue == nil {
					break out
				}
				dequeue = nil
			}

		case err := <-rescanErr:
			if err != nil {
				log.Errorf("Neutrino rescan ended with error: %s", err)
			}

		case s.currentBlock <- bs:

		case <-s.quit:
			break out
		}
	}

	s.Stop()
	close(s.dequeueNotification)
	s.wg.Done()
}
