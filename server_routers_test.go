package main

import (
	"context"
	"crypto/rand"
	"errors"
	"testing"
	"time"

	"github.com/ipfs/boxo/ipns"
	"github.com/ipfs/boxo/path"
	"github.com/ipfs/boxo/routing/http/types"
	"github.com/ipfs/boxo/routing/http/types/iter"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/routing"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type mockRouter struct{ mock.Mock }

var _ router = &mockRouter{}

func (m *mockRouter) FindProviders(ctx context.Context, key cid.Cid, limit int) (iter.ResultIter[types.Record], error) {
	args := m.Called(ctx, key, limit)
	if arg0 := args.Get(0); arg0 == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(iter.ResultIter[types.Record]), args.Error(1)
}

func (m *mockRouter) FindPeers(ctx context.Context, pid peer.ID, limit int) (iter.ResultIter[*types.PeerRecord], error) {
	args := m.Called(ctx, pid, limit)
	if arg0 := args.Get(0); arg0 == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(iter.ResultIter[*types.PeerRecord]), args.Error(1)
}

func (m *mockRouter) GetClosestPeers(ctx context.Context, key cid.Cid) (iter.ResultIter[*types.PeerRecord], error) {
	args := m.Called(ctx, key)
	if arg0 := args.Get(0); arg0 == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(iter.ResultIter[*types.PeerRecord]), args.Error(1)
}

func (m *mockRouter) GetIPNS(ctx context.Context, name ipns.Name) (*ipns.Record, error) {
	args := m.Called(ctx, name)
	if arg0 := args.Get(0); arg0 == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*ipns.Record), args.Error(1)
}

func (m *mockRouter) PutIPNS(ctx context.Context, name ipns.Name, record *ipns.Record) error {
	args := m.Called(ctx, name, record)
	return args.Error(0)
}

func makeName(t *testing.T) (crypto.PrivKey, ipns.Name) {
	sk, _, err := crypto.GenerateEd25519Key(rand.Reader)
	require.NoError(t, err)

	pid, err := peer.IDFromPrivateKey(sk)
	require.NoError(t, err)

	return sk, ipns.NameFromPeer(pid)
}

func makeIPNSRecord(t *testing.T, sk crypto.PrivKey, opts ...ipns.Option) (*ipns.Record, []byte) {
	cid, err := cid.Decode("bafkreifjjcie6lypi6ny7amxnfftagclbuxndqonfipmb64f2km2devei4")
	require.NoError(t, err)

	path := path.FromCid(cid)
	eol := time.Now().Add(time.Hour * 48)
	ttl := time.Second * 20

	record, err := ipns.NewRecord(sk, path, 1, eol, ttl, opts...)
	require.NoError(t, err)

	rawRecord, err := ipns.MarshalRecord(record)
	require.NoError(t, err)

	return record, rawRecord
}

func makeExpiredIPNSRecord(t *testing.T, sk crypto.PrivKey) *ipns.Record {
	cid, err := cid.Decode("bafkreifjjcie6lypi6ny7amxnfftagclbuxndqonfipmb64f2km2devei4")
	require.NoError(t, err)

	// EOL in the past so the record is already expired
	record, err := ipns.NewRecord(sk, path.FromCid(cid), 1, time.Now().Add(-time.Hour), time.Second*20)
	require.NoError(t, err)

	return record
}

func TestGetIPNS(t *testing.T) {
	t.Parallel()

	sk, name := makeName(t)
	rec, _ := makeIPNSRecord(t, sk)

	t.Run("OK (Multiple Composable, One Fails, One OK)", func(t *testing.T) {
		ctx := context.Background()

		mr1 := &mockRouter{}
		mr1.On("GetIPNS", mock.Anything, name).Return(rec, nil)

		mr2 := &mockRouter{}
		mr2.On("GetIPNS", mock.Anything, name).Return(nil, routing.ErrNotFound)

		r := parallelRouter{
			routers: []router{
				composableRouter{
					ipns: mr1,
				},
				composableRouter{
					ipns: mr2,
				},
			},
		}

		getRec, err := r.GetIPNS(ctx, name)
		require.NoError(t, err)
		require.EqualValues(t, rec, getRec)
	})

	t.Run("OK (Multiple Parallel)", func(t *testing.T) {
		ctx := context.Background()

		mr1 := &mockRouter{}
		mr1.On("GetIPNS", mock.Anything, name).Return(nil, routing.ErrNotFound)

		mr2 := &mockRouter{}
		mr2.On("GetIPNS", mock.Anything, name).Return(rec, nil)

		r := parallelRouter{
			routers: []router{
				composableRouter{
					ipns: parallelRouter{
						routers: []router{mr1, mr2},
					},
				},
			},
		}

		getRec, err := r.GetIPNS(ctx, name)
		require.NoError(t, err)
		require.EqualValues(t, rec, getRec)
	})

	t.Run("No Routers", func(t *testing.T) {
		ctx := context.Background()

		r := parallelRouter{
			routers: []router{
				composableRouter{
					ipns: parallelRouter{},
				},
			},
		}

		_, err := r.GetIPNS(ctx, name)
		require.ErrorIs(t, err, routing.ErrNotFound)
	})

	t.Run("Expired Record Treated As Not Found", func(t *testing.T) {
		ctx := context.Background()

		expired := makeExpiredIPNSRecord(t, sk)

		mr1 := &mockRouter{}
		mr1.On("GetIPNS", mock.Anything, name).Return(expired, nil)

		r := parallelRouter{
			routers: []router{
				composableRouter{ipns: mr1},
			},
		}

		_, err := r.GetIPNS(ctx, name)
		require.ErrorIs(t, err, routing.ErrNotFound)
	})

	t.Run("Skips Expired Record For Valid One", func(t *testing.T) {
		ctx := context.Background()

		expired := makeExpiredIPNSRecord(t, sk)

		mr1 := &mockRouter{}
		mr1.On("GetIPNS", mock.Anything, name).Return(expired, nil)

		mr2 := &mockRouter{}
		mr2.On("GetIPNS", mock.Anything, name).Return(rec, nil)

		r := parallelRouter{
			routers: []router{
				composableRouter{ipns: mr1},
				composableRouter{ipns: mr2},
			},
		}

		getRec, err := r.GetIPNS(ctx, name)
		require.NoError(t, err)
		require.EqualValues(t, rec, getRec)
	})
}

func TestPutIPNS(t *testing.T) {
	t.Parallel()

	sk, name := makeName(t)
	rec, _ := makeIPNSRecord(t, sk)

	t.Run("OK (Multiple Composable)", func(t *testing.T) {
		ctx := context.Background()

		mr1 := &mockRouter{}
		mr1.On("PutIPNS", mock.Anything, name, rec).Return(nil)

		mr2 := &mockRouter{}
		mr2.On("PutIPNS", mock.Anything, name, rec).Return(nil)

		r := parallelRouter{
			routers: []router{
				composableRouter{
					ipns: mr1,
				},
				composableRouter{
					ipns: mr2,
				},
			},
		}

		err := r.PutIPNS(ctx, name, rec)
		require.NoError(t, err)

		mr1.AssertExpectations(t)
		mr2.AssertExpectations(t)
	})

	t.Run("OK (Multiple Parallel)", func(t *testing.T) {
		ctx := context.Background()

		mr1 := &mockRouter{}
		mr1.On("PutIPNS", mock.Anything, name, rec).Return(nil)

		mr2 := &mockRouter{}
		mr2.On("PutIPNS", mock.Anything, name, rec).Return(nil)

		r := parallelRouter{
			routers: []router{
				composableRouter{
					ipns: parallelRouter{
						routers: []router{mr1, mr2},
					},
				},
			},
		}

		err := r.PutIPNS(ctx, name, rec)
		require.NoError(t, err)

		mr1.AssertExpectations(t)
		mr2.AssertExpectations(t)
	})

	t.Run("Failure of a Single Router (Multiple Composable)", func(t *testing.T) {
		ctx := context.Background()

		mr1 := &mockRouter{}
		mr1.On("PutIPNS", mock.Anything, name, rec).Return(errors.New("failed"))

		mr2 := &mockRouter{}
		mr2.On("PutIPNS", mock.Anything, name, rec).Return(nil)

		r := parallelRouter{
			routers: []router{
				composableRouter{
					ipns: mr1,
				},
				composableRouter{
					ipns: mr2,
				},
			},
		}

		err := r.PutIPNS(ctx, name, rec)
		require.ErrorContains(t, err, "failed")

		mr1.AssertExpectations(t)
		mr2.AssertExpectations(t)
	})

	t.Run("Failure of a Single Router (Multiple Parallel)", func(t *testing.T) {
		ctx := context.Background()

		mr1 := &mockRouter{}
		mr1.On("PutIPNS", mock.Anything, name, mock.Anything).Return(errors.New("failed"))

		mr2 := &mockRouter{}
		mr2.On("PutIPNS", mock.Anything, name, mock.Anything).Return(nil)

		r := parallelRouter{
			routers: []router{
				composableRouter{
					ipns: parallelRouter{
						routers: []router{mr1, mr2},
					},
				},
			},
		}

		err := r.PutIPNS(ctx, name, rec)
		require.ErrorContains(t, err, "failed")

		mr1.AssertExpectations(t)
		mr2.AssertExpectations(t)
	})
}

func makeCID() cid.Cid {
	buf := make([]byte, 63)
	_, err := rand.Read(buf)
	if err != nil {
		panic(err)
	}
	mh, err := multihash.Encode(buf, multihash.SHA2_256)
	if err != nil {
		panic(err)
	}
	c := cid.NewCidV1(cid.Raw, mh)
	return c
}

func mustMultiaddr(t *testing.T, s string) types.Multiaddr {
	ma, err := multiaddr.NewMultiaddr(s)
	require.NoError(t, err)
	return types.Multiaddr{Multiaddr: ma}
}

func TestFindProviders(t *testing.T) {
	t.Parallel()

	t.Run("Basic", func(t *testing.T) {
		ctx := context.Background()
		c := makeCID()
		peers := []peer.ID{"peer1", "peer2", "peer3"}

		var d router
		d = parallelRouter{}
		it, err := d.FindProviders(ctx, c, 10)
		require.NoError(t, err)
		defer it.Close()
		require.False(t, it.Next())

		mr1 := &mockRouter{}
		mr1Iter := newMockIter[types.Record](ctx)
		mr1.On("FindProviders", mock.Anything, c, 10).Return(mr1Iter, nil)

		mr2 := &mockRouter{}
		mr2Iter := newMockIter[types.Record](ctx)
		mr2.On("FindProviders", mock.Anything, c, 10).Return(mr2Iter, nil)

		d = sanitizeRouter{parallelRouter{
			routers: []router{
				&composableRouter{
					providers: mr1,
				},
				mr2,
			},
		}}

		privateAddr := mustMultiaddr(t, "/ip4/192.168.1.123/tcp/4001")
		loopbackAddr := mustMultiaddr(t, "/ip4/127.0.0.1/tcp/4001")
		publicAddr := mustMultiaddr(t, "/ip4/137.21.14.12/tcp/4001")

		go func() {
			mr1Iter.ch <- iter.Result[types.Record]{Val: &types.PeerRecord{
				Schema: "peer",
				ID:     &peers[0],
				Addrs:  []types.Multiaddr{privateAddr, loopbackAddr, publicAddr},
			}}
			mr2Iter.ch <- iter.Result[types.Record]{Val: &types.PeerRecord{Schema: "peer", ID: &peers[0]}}
			mr1Iter.ch <- iter.Result[types.Record]{Val: &types.PeerRecord{Schema: "peer", ID: &peers[1]}}
			mr1Iter.ch <- iter.Result[types.Record]{Val: &types.PeerRecord{Schema: "peer", ID: &peers[2]}}
			close(mr1Iter.ch)

			mr2Iter.ch <- iter.Result[types.Record]{Val: &types.PeerRecord{Schema: "peer", ID: &peers[1]}}
			close(mr2Iter.ch)
		}()

		it, err = d.FindProviders(ctx, c, 10)
		require.NoError(t, err)
		defer it.Close()

		results, err := iter.ReadAllResults(it)
		require.NoError(t, err)
		require.Len(t, results, 5)

		// The parallelRouter uses manyIter which merges results from multiple routers concurrently.
		// Both mr1 and mr2 send a record for peers[0]:
		// - mr1 sends peers[0] WITH addresses (private, loopback, public)
		// - mr2 sends peers[0] WITHOUT addresses
		// Due to concurrent execution, either record could arrive first in the results.
		// The parallelRouter doesn't deduplicate, so both records are included.
		// We need to find the record that has addresses to verify the sanitizeRouter
		// correctly filtered out private/loopback addresses, keeping only public ones.
		var peerWithAddrs *types.PeerRecord
		for _, r := range results {
			pr := r.(*types.PeerRecord)
			if *pr.ID == peers[0] && len(pr.Addrs) > 0 {
				peerWithAddrs = pr
				break
			}
		}
		require.NotNil(t, peerWithAddrs, "should have found peer[0] with addresses")
		require.Len(t, peerWithAddrs.Addrs, 1)
		require.Equal(t, publicAddr.String(), peerWithAddrs.Addrs[0].String())
	})

	t.Run("Failed to Create All Iterators", func(t *testing.T) {
		ctx := context.Background()
		c := makeCID()

		mr1 := &mockRouter{}
		mr1.On("FindProviders", mock.Anything, c, 10).Return(nil, errors.New("error a"))

		mr2 := &mockRouter{}
		mr2.On("FindProviders", mock.Anything, c, 10).Return(nil, errors.New("error b"))

		d := parallelRouter{
			routers: []router{
				mr1, mr2,
			},
		}

		_, err := d.FindProviders(ctx, c, 10)
		require.ErrorContains(t, err, "error a")
		require.ErrorContains(t, err, "error b")
	})

	t.Run("Failed to Create One Iterator", func(t *testing.T) {
		ctx := context.Background()
		pid := peer.ID("hello")
		c := makeCID()

		mr1 := &mockRouter{}
		mr1.On("FindProviders", mock.Anything, c, 10).Return(iter.ToResultIter(iter.FromSlice([]types.Record{&types.PeerRecord{Schema: "peer", ID: &pid}})), nil)

		mr2 := &mockRouter{}
		mr2.On("FindProviders", mock.Anything, c, 10).Return(nil, errors.New("error b"))

		d := parallelRouter{
			routers: []router{
				mr1, mr2,
			},
		}

		it, err := d.FindProviders(ctx, c, 10)
		require.NoError(t, err)
		defer it.Close()

		results, err := iter.ReadAllResults(it)
		require.NoError(t, err)
		require.Len(t, results, 1)
	})
}

func TestFindPeers(t *testing.T) {
	t.Parallel()

	t.Run("Basic", func(t *testing.T) {
		ctx := context.Background()
		pid := peer.ID("hello")

		d := parallelRouter{}
		it, err := d.FindPeers(ctx, pid, 10)
		require.NoError(t, err)
		defer it.Close()
		require.False(t, it.Next())

		mr1 := &mockRouter{}
		mr1Iter := newMockIter[*types.PeerRecord](ctx)
		mr1.On("FindPeers", mock.Anything, pid, 10).Return(mr1Iter, nil)

		mr2 := &mockRouter{}
		mr2Iter := newMockIter[*types.PeerRecord](ctx)
		mr2.On("FindPeers", mock.Anything, pid, 10).Return(mr2Iter, nil)

		d = parallelRouter{
			routers: []router{
				&composableRouter{
					peers: mr1,
				},
				mr2,
			},
		}

		go func() {
			mr1Iter.ch <- iter.Result[*types.PeerRecord]{Val: &types.PeerRecord{Schema: "peer", ID: &pid}}
			mr2Iter.ch <- iter.Result[*types.PeerRecord]{Val: &types.PeerRecord{Schema: "peer", ID: &pid}}
			mr1Iter.ch <- iter.Result[*types.PeerRecord]{Val: &types.PeerRecord{Schema: "peer", ID: &pid}}
			mr1Iter.ch <- iter.Result[*types.PeerRecord]{Val: &types.PeerRecord{Schema: "peer", ID: &pid}}
			close(mr1Iter.ch)

			mr2Iter.ch <- iter.Result[*types.PeerRecord]{Val: &types.PeerRecord{Schema: "peer", ID: &pid}}
			close(mr2Iter.ch)
		}()

		it, err = d.FindPeers(ctx, pid, 10)
		require.NoError(t, err)
		defer it.Close()

		results, err := iter.ReadAllResults(it)
		require.NoError(t, err)
		require.Len(t, results, 5)
	})

	t.Run("Failed to Create All Iterators", func(t *testing.T) {
		ctx := context.Background()
		pid := peer.ID("hello")

		mr1 := &mockRouter{}
		mr1.On("FindPeers", mock.Anything, pid, 10).Return(nil, errors.New("error a"))

		mr2 := &mockRouter{}
		mr2.On("FindPeers", mock.Anything, pid, 10).Return(nil, errors.New("error b"))

		d := parallelRouter{
			routers: []router{
				mr1, mr2,
			},
		}

		_, err := d.FindPeers(ctx, pid, 10)
		require.ErrorContains(t, err, "error a")
		require.ErrorContains(t, err, "error b")
	})

	t.Run("Failed to Create One Iterator", func(t *testing.T) {
		ctx := context.Background()
		pid := peer.ID("hello")

		mr1 := &mockRouter{}
		mr1.On("FindPeers", mock.Anything, pid, 10).Return(iter.ToResultIter(iter.FromSlice([]*types.PeerRecord{{Schema: "peer", ID: &pid}})), nil)

		mr2 := &mockRouter{}
		mr2.On("FindPeers", mock.Anything, pid, 10).Return(nil, errors.New("error b"))

		d := parallelRouter{
			routers: []router{
				mr1, mr2,
			},
		}

		it, err := d.FindPeers(ctx, pid, 10)
		require.NoError(t, err)
		defer it.Close()

		results, err := iter.ReadAllResults(it)
		require.NoError(t, err)
		require.Len(t, results, 1)
	})
}

type mockIter[T any] struct {
	ctx     context.Context
	ch      chan iter.Result[T]
	waitVal chan time.Time
	val     iter.Result[T]
	done    bool
}

var _ iter.ResultIter[int] = &mockIter[int]{}

func newMockIter[T any](ctx context.Context) *mockIter[T] {
	it := &mockIter[T]{
		ctx: ctx,
		ch:  make(chan iter.Result[T]),
	}

	return it
}

func newMockIters[T any](ctx context.Context, count int) []*mockIter[T] {
	var arr []*mockIter[T]

	for count > 0 {
		arr = append(arr, newMockIter[T](ctx))
		count--
	}

	return arr
}

func (m *mockIter[T]) Next() bool {
	if m.done {
		return false
	}

	select {
	case v, ok := <-m.ch:
		if !ok {
			m.done = true
		} else {
			m.val = v
		}
	case <-m.ctx.Done():
		m.done = true
	}

	return !m.done
}

func (m *mockIter[T]) Val() iter.Result[T] {
	if m.waitVal != nil {
		<-m.waitVal
	}

	return m.val
}

func (m *mockIter[T]) Close() error {
	m.done = true
	return nil
}

func mockItersAsInterface[T any](originalSlice []*mockIter[T]) []iter.ResultIter[T] {
	var newSlice []iter.ResultIter[T]

	for _, v := range originalSlice {
		newSlice = append(newSlice, v)
	}

	return newSlice
}

func TestManyIter(t *testing.T) {
	t.Parallel()

	t.Run("Sequence", func(t *testing.T) {
		t.Parallel()

		ctx := t.Context()

		its := newMockIters[int](ctx, 2)
		manyIter := newManyIter(ctx, mockItersAsInterface(its), nil)

		go func() {
			its[0].ch <- iter.Result[int]{Val: 0}
			time.Sleep(time.Millisecond * 50)

			its[1].ch <- iter.Result[int]{Val: 1}
			time.Sleep(time.Millisecond * 50)

			its[0].ch <- iter.Result[int]{Val: 0}
			time.Sleep(time.Millisecond * 50)

			its[0].ch <- iter.Result[int]{Val: 0}
			close(its[0].ch)
			time.Sleep(time.Millisecond * 50)

			its[1].ch <- iter.Result[int]{Val: 1}
			time.Sleep(time.Millisecond * 50)

			close(its[1].ch)
		}()

		results, err := iter.ReadAllResults(manyIter)
		require.NoError(t, err)
		require.Equal(t, []int{0, 1, 0, 0, 1}, results)
		require.False(t, manyIter.Next())
		require.NoError(t, manyIter.Close())
	})

	t.Run("Closed Iterator", func(t *testing.T) {
		t.Parallel()

		ctx := t.Context()

		its := newMockIters[int](ctx, 5)
		manyIter := newManyIter(ctx, mockItersAsInterface(its), nil)

		go func() {
			close(its[0].ch)
			close(its[1].ch)
			close(its[2].ch)
			close(its[3].ch)

			its[4].ch <- iter.Result[int]{Val: 4}
			time.Sleep(time.Millisecond * 50)

			its[4].ch <- iter.Result[int]{Val: 4}
			time.Sleep(time.Millisecond * 50)

			close(its[4].ch)
		}()

		results, err := iter.ReadAllResults(manyIter)
		require.NoError(t, err)
		require.Equal(t, []int{4, 4}, results)
		require.False(t, manyIter.Next())
		require.NoError(t, manyIter.Close())
	})

	t.Run("Context Canceled", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		its := newMockIters[int](ctx, 5)
		manyIter := newManyIter(ctx, mockItersAsInterface(its), nil)

		go func() {
			its[3].ch <- iter.Result[int]{Val: 3}
			time.Sleep(time.Millisecond * 50)

			its[2].ch <- iter.Result[int]{Val: 2}
			time.Sleep(time.Millisecond * 50)

			cancel()
		}()

		results, err := iter.ReadAllResults(manyIter)
		require.NoError(t, err)
		require.Equal(t, []int{3, 2}, results)
		require.False(t, manyIter.Next())
		require.NoError(t, manyIter.Close())
	})

	t.Run("Context Canceled After .Next Returns", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		its := newMockIters[int](ctx, 5)
		manyIter := newManyIter(ctx, mockItersAsInterface(its), nil)

		go func() {
			its[1].ch <- iter.Result[int]{Val: 1}
			time.Sleep(time.Millisecond * 50)

			its[4].ch <- iter.Result[int]{Val: 4}
			time.Sleep(time.Millisecond * 50)

			its[3].waitVal = make(chan time.Time)
			its[3].ch <- iter.Result[int]{Val: 3}
			time.Sleep(time.Millisecond * 50)

			cancel()
			time.Sleep(time.Millisecond * 50)

			its[3].waitVal <- time.Now()
		}()

		results, err := iter.ReadAllResults(manyIter)
		require.NoError(t, err)
		require.Equal(t, []int{1, 4}, results)
		require.False(t, manyIter.Next())
		require.NoError(t, manyIter.Close())
	})
}

func TestFilterPrivateMultiaddrSortsAndFilters(t *testing.T) {
	mustAddr := func(s string) types.Multiaddr {
		m, err := multiaddr.NewMultiaddr(s)
		require.NoError(t, err)
		return types.Multiaddr{Multiaddr: m}
	}

	priv := mustAddr("/ip4/192.168.1.5/tcp/4001")
	input := []types.Multiaddr{
		mustAddr("/ip4/9.9.9.9/tcp/4001"),
		priv, // private, must be filtered out
		mustAddr("/ip4/1.1.1.1/udp/4001/quic-v1"),
		mustAddr("/ip4/5.5.5.5/tcp/4001"),
	}

	// Shuffle into several orders; the output must be identical every time and
	// must never contain the private address.
	var want []string
	for _, order := range [][]int{{0, 1, 2, 3}, {3, 2, 1, 0}, {2, 0, 3, 1}, {1, 3, 0, 2}} {
		in := make([]types.Multiaddr, 0, len(order))
		for _, i := range order {
			in = append(in, input[i])
		}

		out := filterPrivateMultiaddr(in)

		got := make([]string, 0, len(out))
		for _, a := range out {
			got = append(got, a.String())
			require.NotEqual(t, priv.String(), a.String(), "private addr leaked")
		}
		require.Len(t, got, 3)

		if want == nil {
			want = got
		} else {
			require.Equal(t, want, got, "output order is not stable across input orders")
		}
	}
}

func TestFilterPrivateMultiaddrPlacesRelayAddrsLast(t *testing.T) {
	mustAddr := func(s string) types.Multiaddr {
		m, err := multiaddr.NewMultiaddr(s)
		require.NoError(t, err)
		return types.Multiaddr{Multiaddr: m}
	}

	relay := "/p2p/12D3KooWCZ67sU8oCvKd82Y6c9NgpqgoZYuZEUcg4upHCjK3n1aj/p2p-circuit"
	input := []types.Multiaddr{
		mustAddr("/ip4/9.9.9.9/udp/4001/quic-v1" + relay),
		mustAddr("/ip4/1.1.1.1/tcp/4001"),
		mustAddr("/ip4/5.5.5.5/tcp/4001" + relay),
		mustAddr("/ip4/2.2.2.2/udp/4001/quic-v1"),
	}

	out := filterPrivateMultiaddr(input)
	require.Len(t, out, 4)

	// Direct addresses come first, relay (/p2p-circuit) addresses last.
	require.False(t, isRelayAddr(out[0].Multiaddr))
	require.False(t, isRelayAddr(out[1].Multiaddr))
	require.True(t, isRelayAddr(out[2].Multiaddr))
	require.True(t, isRelayAddr(out[3].Multiaddr))
}

// ---------------------------------------------------------------------------
// Per-router timing.
//
// The router metrics are package-level collectors that every test in the
// package shares, so these tests assert on before/after deltas rather than on
// absolute values.

// timedIter yields a fixed list of records, sleeping before the one at delayAt
// and again before it reports that it has run out. Unlike mockIter it exhausts
// on its own schedule, which is what the timing assertions need: the point is
// to make one router finish later than the other.
type timedIter struct {
	vals     []iter.Result[types.Record]
	delay    time.Duration
	delayAt  int
	delayEnd time.Duration
	i        int
}

var _ iter.ResultIter[types.Record] = (*timedIter)(nil)

func (t *timedIter) Next() bool {
	if t.i >= len(t.vals) {
		if t.delayEnd > 0 {
			time.Sleep(t.delayEnd)
			t.delayEnd = 0
		}
		return false
	}
	if t.i == t.delayAt && t.delay > 0 {
		time.Sleep(t.delay)
	}
	t.i++
	return true
}

func (t *timedIter) Val() iter.Result[types.Record] { return t.vals[t.i-1] }

func (t *timedIter) Close() error { return nil }

// peerRec is a minimal peer record. peer.ID is a string underneath and
// recordKey never decodes it, so an arbitrary one is enough to stand for
// "the same provider" across two routers.
func peerRec(id string) iter.Result[types.Record] {
	pid := peer.ID(id)
	return iter.Result[types.Record]{Val: &types.PeerRecord{Schema: types.SchemaPeer, ID: &pid}}
}

func counterVal(t *testing.T, c *prometheus.CounterVec, lv ...string) float64 {
	t.Helper()
	return testutil.ToFloat64(c.WithLabelValues(lv...))
}

// histVal reads one histogram series' observation count and sum. testutil has
// no helper for this, so it goes through the gatherer the collectors are
// registered with.
func histVal(t *testing.T, metric string, labels map[string]string) (uint64, float64) {
	t.Helper()
	mfs, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != metric {
			continue
		}
		for _, m := range mf.GetMetric() {
			got := map[string]string{}
			for _, lp := range m.GetLabel() {
				got[lp.GetName()] = lp.GetValue()
			}
			match := true
			for k, v := range labels {
				if got[k] != v {
					match = false
					break
				}
			}
			if match {
				return m.GetHistogram().GetSampleCount(), m.GetHistogram().GetSampleSum()
			}
		}
	}
	return 0, 0
}

func fieldsMap(t *testing.T, fields []any) map[string]any {
	t.Helper()
	require.Zero(t, len(fields)%2, "trace fields must be key/value pairs")
	m := make(map[string]any, len(fields)/2)
	for i := 0; i < len(fields); i += 2 {
		k, ok := fields[i].(string)
		require.True(t, ok, "trace field key %d is not a string", i)
		m[k] = fields[i+1]
	}
	return m
}

func TestRouterTimingExhausted(t *testing.T) {
	const (
		op    = routerOpProviders
		fast  = "delegated:cid.contact"
		slow  = "dht"
		delay = 150 * time.Millisecond
	)

	recBefore := counterVal(t, routerRecords, op, fast)
	recSlowBefore := counterVal(t, routerRecords, op, slow)
	excBefore := counterVal(t, routerExclusiveRecords, op, fast)
	excSlowBefore := counterVal(t, routerExclusiveRecords, op, slow)
	firstBefore, _ := histVal(t, "someguy_router_first_result_seconds", map[string]string{"op": op, "router": fast})
	firstSlowBefore, _ := histVal(t, "someguy_router_first_result_seconds", map[string]string{"op": op, "router": slow})
	doneBefore, _ := histVal(t, "someguy_router_done_seconds", map[string]string{"op": op, "router": fast, "reason": routerDoneExhausted})
	doneSlowBefore, _ := histVal(t, "someguy_router_done_seconds", map[string]string{"op": op, "router": slow, "reason": routerDoneExhausted})
	tailBefore, tailSumBefore := histVal(t, "someguy_router_tail_seconds", map[string]string{"router": slow})
	tailFastBefore, _ := histVal(t, "someguy_router_tail_seconds", map[string]string{"router": fast})
	lastBefore := counterVal(t, routerLastFinisher, slow)

	// "C" is produced by both routers, so it is exclusive to neither.
	fastIt := &timedIter{vals: []iter.Result[types.Record]{peerRec("A"), peerRec("B"), peerRec("C")}}
	slowIt := &timedIter{vals: []iter.Result[types.Record]{peerRec("C"), peerRec("D")}, delay: delay}

	trace := newRouterTrace(op, []string{fast, slow}, time.Now(), true)
	mi := newManyIter(t.Context(), []iter.ResultIter[types.Record]{fastIt, slowIt}, trace)

	got, err := iter.ReadAllResults(mi)
	require.NoError(t, err)
	require.Len(t, got, 5)
	require.NoError(t, mi.Close())

	require.Equal(t, 3.0, counterVal(t, routerRecords, op, fast)-recBefore)
	require.Equal(t, 2.0, counterVal(t, routerRecords, op, slow)-recSlowBefore)

	// A and B for the fast router, D for the slow one; C is shared.
	require.Equal(t, 2.0, counterVal(t, routerExclusiveRecords, op, fast)-excBefore)
	require.Equal(t, 1.0, counterVal(t, routerExclusiveRecords, op, slow)-excSlowBefore)

	firstAfter, _ := histVal(t, "someguy_router_first_result_seconds", map[string]string{"op": op, "router": fast})
	firstSlowAfter, _ := histVal(t, "someguy_router_first_result_seconds", map[string]string{"op": op, "router": slow})
	require.Equal(t, uint64(1), firstAfter-firstBefore, "first result observed once per router that produced")
	require.Equal(t, uint64(1), firstSlowAfter-firstSlowBefore)

	doneAfter, _ := histVal(t, "someguy_router_done_seconds", map[string]string{"op": op, "router": fast, "reason": routerDoneExhausted})
	doneSlowAfter, _ := histVal(t, "someguy_router_done_seconds", map[string]string{"op": op, "router": slow, "reason": routerDoneExhausted})
	require.Equal(t, uint64(1), doneAfter-doneBefore)
	require.Equal(t, uint64(1), doneSlowAfter-doneSlowBefore)

	// The tail belongs to the slow router alone, and is about the delay: the
	// fast one was finished before the slow one produced anything.
	tailAfter, tailSumAfter := histVal(t, "someguy_router_tail_seconds", map[string]string{"router": slow})
	tailFastAfter, _ := histVal(t, "someguy_router_tail_seconds", map[string]string{"router": fast})
	require.Equal(t, uint64(1), tailAfter-tailBefore)
	require.Equal(t, uint64(0), tailFastAfter-tailFastBefore)
	tail := tailSumAfter - tailSumBefore
	require.Greater(t, tail, delay.Seconds()/2)
	require.Less(t, tail, delay.Seconds()*4)
	require.Equal(t, 1.0, counterVal(t, routerLastFinisher, slow)-lastBefore)

	// The trace line: one entry for the request, four per router.
	f := fieldsMap(t, trace.traceFields(trace.snapshot(), false))
	require.Equal(t, op, f["op"])
	require.Equal(t, routerDoneExhausted, f["reason"])
	require.Equal(t, 3, f[fast+"_records"])
	require.Equal(t, 2, f[slow+"_records"])
	require.Equal(t, 2, f[fast+"_exclusive"])
	require.Equal(t, 1, f[slow+"_exclusive"])
	require.GreaterOrEqual(t, f[slow+"_first_ms"], int64(delay.Milliseconds()/2))
	require.GreaterOrEqual(t, f["total_ms"], f[slow+"_done_ms"])
}

func TestRouterTimingCancelled(t *testing.T) {
	const (
		op   = routerOpPeers
		fast = "delegated:cid.contact"
		slow = "dht"
	)

	doneBefore, _ := histVal(t, "someguy_router_done_seconds", map[string]string{"op": op, "router": fast, "reason": routerDoneExhausted})
	doneSlowBefore, _ := histVal(t, "someguy_router_done_seconds", map[string]string{"op": op, "router": slow, "reason": routerDoneCancelled})
	lastBefore := counterVal(t, routerLastFinisher, slow)
	excBefore := counterVal(t, routerExclusiveRecords, op, slow)

	fastIt := &timedIter{vals: []iter.Result[types.Record]{peerRec("A"), peerRec("B"), peerRec("C")}}
	// The slow router hands over one record and then takes 200ms to admit it
	// has no more, so it is still running when Close cancels the request -
	// which is what boxo's server does once the records limit is reached. It
	// deliberately has no second record to offer: Close drains the channel to
	// unblock pending sends, so a record still in hand at the cancel would
	// sometimes go through and sometimes not.
	slowIt := &timedIter{vals: []iter.Result[types.Record]{peerRec("X")}, delayEnd: 200 * time.Millisecond}

	trace := newRouterTrace(op, []string{fast, slow}, time.Now(), true)
	mi := newManyIter(t.Context(), []iter.ResultIter[types.Record]{fastIt, slowIt}, trace)

	// Exactly the four records available without waiting out the delay.
	for range 4 {
		require.True(t, mi.Next())
	}
	// The fast router's goroutine has sent its last record but may not have
	// reached its own finish yet; let it get there, so that the cancel below
	// is unambiguously after it.
	require.Eventually(t, func() bool { return trace.snapshot().finished == 1 }, time.Second, time.Millisecond)
	require.NoError(t, mi.Close())

	doneAfter, _ := histVal(t, "someguy_router_done_seconds", map[string]string{"op": op, "router": fast, "reason": routerDoneExhausted})
	doneSlowAfter, _ := histVal(t, "someguy_router_done_seconds", map[string]string{"op": op, "router": slow, "reason": routerDoneCancelled})
	require.Equal(t, uint64(1), doneAfter-doneBefore, "the router that ran out is exhausted, not cancelled")
	require.Equal(t, uint64(1), doneSlowAfter-doneSlowBefore, "the router still running when Close cancelled is cancelled")

	// finalize ran exactly once, from the closer goroutine, and a later call
	// from Close cannot double-count.
	require.Equal(t, 1.0, counterVal(t, routerLastFinisher, slow)-lastBefore)
	require.Equal(t, 1.0, counterVal(t, routerExclusiveRecords, op, slow)-excBefore, "X, forwarded before the cancel")
	trace.finalize(true)
	require.Equal(t, 1.0, counterVal(t, routerLastFinisher, slow)-lastBefore)
	require.Equal(t, 1.0, counterVal(t, routerExclusiveRecords, op, slow)-excBefore)

	f := fieldsMap(t, trace.traceFields(trace.snapshot(), true))
	require.Equal(t, routerDoneCancelled, f["reason"])
	require.Equal(t, 1, f[slow+"_records"])
	require.Equal(t, 3, f[fast+"_records"])
}

func TestRouterTraceFieldsForSilentRouter(t *testing.T) {
	const (
		op    = "silent-test"
		quiet = "dht"
		other = "delegated:cid.contact"
	)

	trace := newRouterTrace(op, []string{quiet, other}, time.Now(), true)
	trace.record(1, peerRec("A").Val)
	trace.finish(1, false)
	trace.finish(0, false)

	f := fieldsMap(t, trace.traceFields(trace.snapshot(), false))
	require.Equal(t, int64(-1), f[quiet+"_first_ms"], "a router that produced nothing has no first result")
	require.Equal(t, 0, f[quiet+"_records"])
	require.Equal(t, 0, f[quiet+"_exclusive"])
	require.NotEqual(t, int64(-1), f[other+"_first_ms"])
	require.Equal(t, 1, f[other+"_records"])
	require.Equal(t, 1, f[other+"_exclusive"])
}

func TestRouterName(t *testing.T) {
	// The shapes combineRouters actually builds, plus the two that can only
	// turn up by mistake.
	dht := libp2pRouter{}
	require.Equal(t, "dht", routerName(sanitizeRouter{dht}))
	require.Equal(t, "dht", routerName(sanitizeRouter{NewCachedRouter(dht, nil)}))
	require.Equal(t, "dht", routerName(dnsAddrRouter{router: sanitizeRouter{NewCachedRouter(dht, nil)}}))
	require.Equal(t, "delegated:cid.contact", routerName(clientRouter{name: "cid.contact"}))

	// additionalRouters (the HTTP block providers) and anything unrecognised.
	require.Equal(t, "other", routerName(composableRouter{}))
	require.Equal(t, "other", routerName(nil))
}

func TestEndpointLabel(t *testing.T) {
	require.Equal(t, "cid.contact", endpointLabel("https://cid.contact"))
	require.Equal(t, "cid.contact", endpointLabel("https://cid.contact/routing/v1"))
	require.Equal(t, "delegated-ipfs.dev", endpointLabel("https://delegated-ipfs.dev/routing/v1"))
	require.Equal(t, "example.com:8080", endpointLabel("http://example.com:8080/x"))
	// No host to take: keep the whole thing rather than label it "".
	require.Equal(t, "not a url", endpointLabel("not a url"))
}
